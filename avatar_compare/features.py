from __future__ import annotations

from dataclasses import dataclass
import hashlib
import io
from pathlib import Path
import time
import warnings

import cv2
import numpy as np
from PIL import Image, ImageOps, __version__ as pillow_version

FEATURE_VERSION = f'sift512-phash63-jpeg-preview-v2:cv{cv2.__version__}:pil{pillow_version}:np{np.__version__}'
MAX_BYTES = 32 * 1024 * 1024
MAX_PIXELS = 20_000_000


@dataclass
class Features:
    digest: str
    pixels: str
    width: int
    height: int
    work_width: int
    work_height: int
    ahash: int
    phash: int
    gray: np.ndarray
    points: np.ndarray
    descriptors: np.ndarray
    preview: bytes
    elapsed_ms: float


def bits(values: np.ndarray) -> int:
    result = 0
    for value in values.ravel():
        result = (result << 1) | int(bool(value))
    return result


def read_bytes(path: Path) -> bytes:
    with path.open('rb') as stream:
        value = stream.read(MAX_BYTES + 1)
    if len(value) > MAX_BYTES:
        raise ValueError('图片超过 32 MiB 的单文件限制')
    return value


def extract(data: bytes, *, local_features: bool = True) -> Features:
    started = time.perf_counter()
    if len(data) > MAX_BYTES:
        raise ValueError('图片超过 32 MiB 的单文件限制')
    with warnings.catch_warnings():
        warnings.simplefilter('error', Image.DecompressionBombWarning)
        with Image.open(io.BytesIO(data)) as source:
            if source.width * source.height > MAX_PIXELS:
                raise ValueError('图片超过 2000 万像素的解码限制')
            source.seek(0)
            image = ImageOps.exif_transpose(source).convert('RGBA')
            ground = Image.new('RGBA', image.size, (255, 255, 255, 255))
            image = Image.alpha_composite(ground, image).convert('RGB')
    width, height = image.size
    pixels = hashlib.sha256(f'{width}x{height}:'.encode() + image.tobytes()).hexdigest()
    preview_image = image.copy()
    preview_image.thumbnail((480, 480), Image.Resampling.LANCZOS)
    preview_buffer = io.BytesIO()
    preview_image.save(preview_buffer, format='JPEG', quality=86)
    image.thumbnail((512, 512), Image.Resampling.LANCZOS)
    rgb = np.asarray(image)
    gray_full = cv2.cvtColor(rgb, cv2.COLOR_RGB2GRAY)
    small8 = cv2.resize(gray_full, (8, 8), interpolation=cv2.INTER_AREA)
    ahash = bits(small8 > small8.mean(dtype=np.float64))
    small32 = cv2.resize(gray_full, (32, 32), interpolation=cv2.INTER_AREA)
    low = cv2.dct(small32.astype(np.float32))[:8, :8].ravel()[1:]
    phash = bits(low > np.median(low))
    keypoints, desc = cv2.SIFT_create(nfeatures=600).detectAndCompute(gray_full, None) if local_features else ([], None)
    points = np.array([kp.pt for kp in keypoints], dtype=np.float32).reshape(-1, 2)
    if desc is None:
        desc = np.empty((0, 128), dtype=np.float32)
    return Features(
        hashlib.sha256(data).hexdigest(), pixels, width, height, image.width, image.height,
        ahash, phash, cv2.resize(gray_full, (128, 128), interpolation=cv2.INTER_AREA),
        points, desc.astype(np.float32, copy=False), preview_buffer.getvalue(),
        (time.perf_counter() - started) * 1000,
    )


def structural_similarity(left: np.ndarray, right: np.ndarray) -> float:
    # SSIM's local luminance/contrast formula; uint8 arithmetic must be promoted.
    a, b = left.astype(np.float64), right.astype(np.float64)
    mu_a = cv2.GaussianBlur(a, (11, 11), 1.5)
    mu_b = cv2.GaussianBlur(b, (11, 11), 1.5)
    var_a = cv2.GaussianBlur(a * a, (11, 11), 1.5) - mu_a * mu_a
    var_b = cv2.GaussianBlur(b * b, (11, 11), 1.5) - mu_b * mu_b
    covariance = cv2.GaussianBlur(a * b, (11, 11), 1.5) - mu_a * mu_b
    c1, c2 = (0.01 * 255) ** 2, (0.03 * 255) ** 2
    score = ((2 * mu_a * mu_b + c1) * (2 * covariance + c2)) / ((mu_a * mu_a + mu_b * mu_b + c1) * (var_a + var_b + c2))
    return float(np.clip(score[5:-5, 5:-5].mean(), -1, 1))


def geometric_evidence(query: Features, candidate: Features) -> dict:
    evidence = {'ratio_matches': 0, 'inliers': 0, 'inlier_ratio': 0.0, 'query_coverage': 0.0, 'candidate_coverage': 0.0, 'consistent': False}
    if len(query.descriptors) < 4 or len(candidate.descriptors) < 4:
        return evidence
    neighbors = cv2.BFMatcher(cv2.NORM_L2).knnMatch(query.descriptors, candidate.descriptors, k=2)
    good = [pair[0] for pair in neighbors if len(pair) == 2 and pair[0].distance < 0.75 * pair[1].distance]
    # SIFT may assign several orientations to one spatial location. Keypoint
    # indices alone cannot make these independent geometric observations.
    unique, query_sites, candidate_sites = [], set(), set()
    for match in sorted(good, key=lambda item: item.distance):
        query_site = tuple(round(float(value), 2) for value in query.points[match.queryIdx])
        candidate_site = tuple(round(float(value), 2) for value in candidate.points[match.trainIdx])
        if query_site in query_sites or candidate_site in candidate_sites:
            continue
        unique.append(match)
        query_sites.add(query_site)
        candidate_sites.add(candidate_site)
    good = unique
    evidence['ratio_matches'] = len(good)
    if len(good) < 4:
        return evidence
    source = np.array([query.points[m.queryIdx] for m in good], dtype=np.float32)
    destination = np.array([candidate.points[m.trainIdx] for m in good], dtype=np.float32)
    matrix, mask = cv2.findHomography(source, destination, cv2.RANSAC, 3.0, maxIters=2000, confidence=0.995)
    if matrix is None or mask is None or not np.isfinite(matrix).all():
        return evidence
    inside = mask.ravel().astype(bool)
    count = int(inside.sum())
    evidence['inliers'] = count
    evidence['inlier_ratio'] = count / len(good)
    if count >= 3:
        evidence['query_coverage'] = float(cv2.contourArea(cv2.convexHull(source[inside]))) / (query.work_width * query.work_height)
        evidence['candidate_coverage'] = float(cv2.contourArea(cv2.convexHull(destination[inside]))) / (candidate.work_width * candidate.work_height)
    corners = np.array([[[0, 0], [query.work_width, 0], [query.work_width, query.work_height], [0, query.work_height]]], dtype=np.float32)
    projected = cv2.perspectiveTransform(corners, matrix)[0]
    plausible = np.isfinite(projected).all() and cv2.isContourConvex(projected.reshape(-1, 1, 2)) and 4 < abs(cv2.contourArea(projected)) < 100 * candidate.work_width * candidate.work_height
    indices = np.flatnonzero(inside)
    if len(indices):
        indices = indices[np.linspace(0, len(indices)-1, min(16, len(indices)), dtype=int)]
        evidence['point_pairs'] = [
            {'query': (source[index] / [query.work_width, query.work_height]).tolist(), 'candidate': (destination[index] / [candidate.work_width, candidate.work_height]).tolist()}
            for index in indices
        ]
    # Provisional review gate, not an accuracy calibration or identity claim.
    evidence['consistent'] = bool(count >= 8 and evidence['inlier_ratio'] >= 0.5 and min(evidence['query_coverage'], evidence['candidate_coverage']) >= 0.08 and plausible)
    return evidence


def compare(query: Features, candidate: Features, *, geometry: bool = True) -> dict:
    started = time.perf_counter()
    result = {
        'exact_file': query.digest == candidate.digest,
        'exact_pixels': query.pixels == candidate.pixels,
        'ahash_distance': (query.ahash ^ candidate.ahash).bit_count(),
        'phash_distance': (query.phash ^ candidate.phash).bit_count(),
        'ssim': structural_similarity(query.gray, candidate.gray),
    }
    result['ahash_agreement'] = 1 - result['ahash_distance'] / 64
    result['phash_agreement'] = 1 - result['phash_distance'] / 63
    if geometry:
        result['geometry'] = geometric_evidence(query, candidate)
    result['elapsed_ms'] = (time.perf_counter() - started) * 1000
    return result
