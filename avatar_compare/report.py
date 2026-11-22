from __future__ import annotations
import base64
import csv
import html
import io
import json
from pathlib import Path
import tempfile
from PIL import Image

from .engine import Store


def evidence_label(row: dict) -> str:
    metric = row['metrics']
    if metric is None:
        return '读取失败'
    if metric['exact_file']:
        return '文件完全相同'
    if metric['exact_pixels']:
        return '像素完全相同'
    if metric['geometry']['consistent']:
        return '局部位置关系一致（实验性）'
    return '证据不足，需人工核对'


def error_description(row: dict) -> str:
    fetch = row.get('fetch', {})
    status = fetch.get('http_status')
    if status == 404:
        return '接口未返回此编号图片（HTTP 404）；不表示账号不存在'
    if status in {401, 403}:
        return f'接口拒绝授权（HTTP {status}）；检查令牌后用 --retry-errors 继续'
    code = row.get('error') or ''
    return {'http_error': f'接口请求失败（HTTP {status}）；可用 --retry-errors 重试',
            'transport_error': '连接或读取超时；可用 --retry-errors 重试',
            'body_timeout': '响应体读取超时；可用 --retry-errors 重试',
            'response_too_large_or_invalid_length': '响应大小超过限制或长度无效',
            'response_too_large': '响应大小超过限制',
            'incomplete_response': '接口响应未完整接收；可重试',
            'unsupported_content_encoding': '接口响应压缩编码不受支持'}.get(code, code)


def data_url(value: bytes) -> str:
    kind = 'image/jpeg' if value.startswith(b'\xff\xd8') else 'image/png'
    return 'data:' + kind + ';base64,' + base64.b64encode(value).decode()


def safe_cell(value) -> str:
    text = str(value)
    return "'" + text if text.lstrip().startswith(('=', '+', '-', '@', '\t', '\r')) else text


def correspondence_diagram(query: bytes, candidate: bytes, pairs: list[dict]) -> str:
    if not pairs:
        return ''
    def frame(raw, left):
        with Image.open(io.BytesIO(raw)) as image:
            width, height = image.size
        scale = min(440/width, 320/height)
        return left+(460-width*scale)/2, 20+(320-height*scale)/2, width*scale, height*scale
    qx, qy, qw, qh = frame(query, 10)
    cx, cy, cw, ch = frame(candidate, 510)
    parts = [f'<svg class="mapping" viewBox="0 0 980 360" role="img" aria-label="两图局部细节的对应连线"><title>局部细节对应连线</title><image x="{qx}" y="{qy}" width="{qw}" height="{qh}" href="{data_url(query)}"/><image x="{cx}" y="{cy}" width="{cw}" height="{ch}" href="{data_url(candidate)}"/>']
    for pair in pairs[:16]:
        ax, ay = qx+float(pair['query'][0])*qw, qy+float(pair['query'][1])*qh
        bx, by = cx+float(pair['candidate'][0])*cw, cy+float(pair['candidate'][1])*ch
        parts.append(f'<line x1="{ax}" y1="{ay}" x2="{bx}" y2="{by}" stroke="#11765e" stroke-width="1.5" opacity=".65"/><circle cx="{ax}" cy="{ay}" r="3" fill="#11765e" stroke="white"/><circle cx="{bx}" cy="{by}" r="3" fill="#11765e" stroke="white"/>')
    return ''.join(parts)+'</svg>'


def atomic_write(path: Path, content: str, encoding='utf-8'):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile('w', encoding=encoding, dir=path.parent, prefix='.report-', suffix='.tmp', delete=False, newline='') as stream:
        temporary = Path(stream.name).resolve()
        if temporary.parent != path.parent.resolve():
            raise ValueError('临时输出路径不在报告目录内')
        stream.write(content)
    temporary.replace(path)


def write_reports(result: dict, output: Path, *, limit=20) -> dict[str, Path]:
    output = output.resolve()
    limit = max(1, min(int(limit), 100))
    stem = 'comparison-' + result['run_id']
    paths = {kind: output/(stem + '.' + extension) for kind, extension in [('html', 'html'), ('json', 'json'), ('csv', 'csv')]}
    remote = 'range' in result
    disk = result.get('result_storage') == 'disk'
    compact = result.get('retention_mode') == 'compact'
    if remote:
        paths['errors'] = output/(stem+'.errors.jsonl')
    store = Store(Path(result['state_dir'])/'cache.sqlite3')
    try:
        query_bytes = store.preview(result['target_digest'])
        query_preview = data_url(query_bytes)
        figures = []
        csv_data = io.StringIO(newline='')
        writer = csv.writer(csv_data)
        writer.writerow(['排序', '来源文件', '证据类型', '结构相似度', '感知哈希一致度', '均值哈希一致度', '几何内点数', '错误'] + (['编号', 'HTTP状态', '获取尝试数'] if remote else []))
        for index, row in enumerate(result['results'], 1):
            metric = row['metrics']
            name = '编号 '+row['identifier'] if remote else str(Path(row['path']).relative_to(result['folder']))
            label = evidence_label(row)
            writer.writerow([index, safe_cell(name), label, metric['ssim'] if metric else '', metric['phash_agreement'] if metric else '', metric['ahash_agreement'] if metric else '', metric['geometry']['inliers'] if metric else '', safe_cell(error_description(row))] + ([row['identifier'], row['fetch']['http_status'], row['fetch']['attempts']] if remote else []))
            if index > max(1, min(limit, 100)):
                continue
            if metric is None:
                figures.append(f'<li class="result"><h2>{index}. {html.escape(name)}</h2><p class="error">{html.escape(error_description(row))}</p></li>')
                continue
            candidate_bytes = store.preview(row['digest'])
            candidate_preview = data_url(candidate_bytes)
            geometry = metric['geometry']
            pairs = geometry.get('point_pairs', [])
            pair_text = '精确内容一致，无需几何复核' if metric['exact_file'] or metric['exact_pixels'] else f'{geometry["inliers"]} 个一致点 / {geometry["ratio_matches"]} 个候选点'
            # Raster previews are user-selected data; never external assets.
            figures.append(f'''<li class="result">
              <div class="row"><h2>{index}. {html.escape(name)}</h2><span class="badge">{html.escape(label)}</span></div>
              <div class="comparison"><figure><img src="{query_preview}" alt="目标图片"><figcaption>目标图片</figcaption></figure><figure><img src="{candidate_preview}" alt="候选图片"><figcaption>候选图片</figcaption></figure></div>
              <dl><div><dt>结构相似度</dt><dd>{metric['ssim']:.3f}</dd></div><div><dt>感知哈希一致度</dt><dd>{metric['phash_agreement']:.1%}</dd></div><div><dt>局部几何证据</dt><dd>{pair_text}</dd></div></dl>
              <details><summary>查看对应细节与依据</summary><p>目标覆盖 {geometry['query_coverage']:.1%}，候选覆盖 {geometry['candidate_coverage']:.1%}。全局分数低也保留复核，避免漏掉裁剪、截图和镜像副本。</p>{correspondence_diagram(query_bytes, candidate_bytes, pairs)}<p>{'连线标出了部分能互相对应的局部细节；可检查它们是否分布在相同图案中。' if pairs else '此结果没有局部点对应；文件或像素完全相同时不需要几何复核。'}</p><p>几何一致性使用暂定门槛，尚未用复杂头像库标定；不构成账号或人物归属结论。</p></details>
            </li>''')
        stats = result['stats']
        location = ('来源：'+result['source']['name']+' · 缓存批次：'+result['source']['revision']+' · 编号范围：'+result['range']['start']+'–'+result['range']['end']+'（含两端）') if remote else '资料夹：'+result['folder']
        remote_note = f"<p>请求 {stats.get('requests', 0)} 次。所有失败编号与尝试记录均保存在同名 errors.jsonl；--retry-errors 只重试失败项，--refresh-source 重新获取整个范围。401/403 会停止任务。</p><p>图片是接口获取时的内容，缓存批次不是服务端固定快照。同一头像可能被多个编号使用，默认头像也可能重复；HTTP 200 和图片相同均不能证明账号存在或头像归属。</p>" if remote else ''
        if disk:
            command = f'python -m avatar_compare.ledger --state-dir "{result["state_dir"]}" --run-id {result["run_id"]}'
            remote_note = f'<p>全速模式：下载并行 {stats["download_workers"]}、比对并行 {stats["workers"]}；实际请求 {stats["requests"]} 次。完整 {stats["processed"]} 项记录持续保存在磁盘；本页与同名CSV/JSON仅包含排名靠前的 {len(result["results"])} 项，errors.jsonl也仅对应这些展示项。完整逐项记录含全部失败可用下列命令流式导出为CSV和JSONL：</p><pre>{html.escape(command)}</pre><p>--retry-errors 只重试失败项；--refresh-source 重新获取整个范围。授权失败或持续429会停止任务。缓存批次不是服务端固定快照；重复头像不能证明账号归属。</p>'
            if compact:
                remote_note = f'<p>省空间模式：已处理 {stats["processed"]} 项的区间已提交；其中失败 {stats["errors"]} 项，失败编号不代表成功比对。保留有匹配证据的候选及当前目标全局前100项复核图片；其余未命中原图、特征及完整逐项分数不保存。同名errors.jsonl包含本范围的全部失败。</p><p>相同目标、缓存批次和算法下可跨范围跳过已处理项；--retry-errors 重试失败项，--refresh-source 开始新批次。缩小范围不能恢复已丢弃的未命中排名，更换目标通常需要重新获取。可导出保留记录、完成区间和错误；加 --images 只导出当前保留的原图：</p><pre>{html.escape(command)}</pre><p>HTTP200和重复头像不能证明账号存在或归属。已有完整缓存不会被此模式自动清理。</p>'
        title = '编号头像比对结果' if remote else '本地图片比对结果'
        status = '已完成' if result['status'] == 'complete' else '已停止，可按相同输入继续'
        if remote and result['status'] == 'complete' and stats['errors']:
            status = '已完成（含失败项，可重试）'
        empty = ('<p class="empty">尚未完成任何编号。可使用相同输入继续。</p>' if remote else '<p class="empty">所选文件夹中没有可比较的图片。目标文件自身会跳过；可选择包含其他图片的文件夹。</p>') if not result['results'] else ''
        if compact and stats['processed'] and not result['results']:
            empty='<p class="empty">当前范围没有保留的候选或复核图片；已处理区间仍可恢复和导出。</p>'
        detail_note = '<p>目标图片中可辨认的局部细节较少，位置关系验证可能不足，请结合并排图片核对。</p>' if stats.get('query_keypoints', 0) < 8 else ''
        dataset_note = '<p><strong>'+html.escape(result['dataset_note'])+'</strong></p>' if result.get('dataset_note') else ''
        output_note = f'显示前 {min(len(result["results"]), limit)} 项；完整逐项记录按任务保存，可流式导出。' if disk else f'显示前 {min(len(result["results"]), limit)} 项，其余记录保存在同名 CSV 和 JSON 中。'
        if compact:output_note=f'显示保留记录中的前 {min(len(result["results"]),limit)} 项；非命中仅保留完成区间。'
        document = f'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="Content-Security-Policy" content="default-src 'none'; img-src data:; style-src 'unsafe-inline'"><title>{title}</title><style>.mapping{{display:block;width:100%;height:auto;margin:14px 0;background:#ffffff}}</style>
        <style>*{{box-sizing:border-box}}body{{margin:0;background:#f5f7f7;color:#19312e;font:16px/1.65 "Segoe UI","Microsoft YaHei",sans-serif}}main{{max-width:1040px;margin:auto;padding:36px 24px 72px}}h1{{font-size:26px;line-height:1.35;margin:0 0 12px}}h2{{font-size:17px;margin:0;overflow-wrap:anywhere}}p{{overflow-wrap:anywhere;margin:8px 0;max-width:75ch}}.muted,dt,figcaption{{color:#526862}}header{{margin-bottom:32px}}.status{{font-weight:600;color:#176756}}.result{{list-style:none;border-top:1px solid #ccd8d4;padding:28px 0}}ol{{padding:0;margin:0}}.row{{display:flex;justify-content:space-between;gap:16px;align-items:baseline}}.badge{{font-size:14px;color:#176756;flex-shrink:0}}.comparison{{display:grid;grid-template-columns:1fr 1fr;gap:24px;margin:20px 0}}figure{{margin:0;text-align:center;background:#fff;border:1px solid #dae3df;padding:14px}}img{{display:block;width:100%;height:280px;object-fit:contain}}figcaption{{margin-top:8px;font-size:14px}}dl{{display:flex;gap:32px;flex-wrap:wrap;margin:12px 0}}dl div{{min-width:140px}}dt{{font-size:14px}}dd{{margin:2px 0;font-weight:600;font-variant-numeric:tabular-nums}}summary{{cursor:pointer;color:#176756;text-underline-offset:3px}}summary:focus-visible{{outline:2px solid #176756;outline-offset:4px}}pre{{white-space:pre-wrap;max-height:180px;overflow:auto;background:#edf2ef;padding:12px;font:12px/1.5 Consolas,monospace}}.error{{color:#a23030;overflow-wrap:anywhere}}.empty{{padding:24px 0}}::selection{{background:#cee8de;color:#19312e}}@media(max-width:600px){{main{{padding:24px 14px}}.row{{display:block}}.comparison{{gap:10px}}figure{{padding:8px}}img{{height:190px}}dl{{gap:12px}}dl div{{min-width:120px}}}}</style></head><body><main>
        <header><h1>{title}</h1><p class="status">{status} · 已处理 {stats['processed']} / {stats['total']} 张 · 用时 {stats['elapsed_seconds']:.2f} 秒</p>{dataset_note}<p class="muted">目标：{html.escape(Path(result['target']).name)}<br>{html.escape(location)}</p><p>这里比较的是图片是否同源。结构、哈希和局部位置关系分别展示；没有把它们合成为未经标定的“身份概率”。{output_note}</p><p class="muted">复用已完成结果 {stats['resumed']} 项；特征缓存命中 {stats['feature_hits']} 项；读取失败 {stats['errors']} 项。图片和报告均留在本机。</p>{remote_note}{detail_note}</header>{empty}<ol>{''.join(figures)}</ol></main></body></html>'''
        atomic_write(paths['html'], document)
        atomic_write(paths['json'], json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))
        atomic_write(paths['csv'], csv_data.getvalue(), encoding='utf-8-sig')
        if remote:
            if compact:
                from .history import error_rows
                with tempfile.NamedTemporaryFile('w',encoding='utf-8',dir=output,prefix='.errors-',suffix='.tmp',delete=False) as stream:
                    temporary=Path(stream.name)
                    for _,row in error_rows(store.db(),result['scope_id'],int(result['range']['start']),int(result['range']['end'])):
                        stream.write(json.dumps({'identifier':row['identifier'],'error':row['error'],'fetch':row['fetch']},ensure_ascii=False)+'\n')
                temporary.replace(paths['errors'])
            else:atomic_write(paths['errors'], ''.join(json.dumps({'identifier': row['identifier'], 'error': row['error'], 'fetch': row['fetch']}, ensure_ascii=False)+'\n' for row in result['results'] if row['error']))
        return paths
    finally:
        store.close()
