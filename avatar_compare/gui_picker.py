"""Native folder dialog in its own main thread/process."""
import json
import argparse
import sys
import tkinter as tk
from tkinter import filedialog


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--kind', choices=['target', 'folder'], default='folder')
    args = parser.parse_args()
    root = tk.Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    try:
        value = filedialog.askopenfilename(title='选择原始目标图片', filetypes=[('图片', '*.png *.jpg *.jpeg *.webp *.bmp *.tif *.tiff *.gif')], parent=root) if args.kind == 'target' else filedialog.askdirectory(title='选择候选图片文件夹', parent=root)
        sys.stdout.buffer.write(json.dumps(value, ensure_ascii=False).encode('utf-8'))
    finally:
        root.destroy()


if __name__ == '__main__':
    main()
