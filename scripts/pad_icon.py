import sys
import os
from PyQt6.QtGui import QImage, QPainter, QColor
from PyQt6.QtCore import Qt, QSize

def pad_icon():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    img_path = os.path.join(base, 'frontend', 'public', 'assets', 'unt-logo.png')
    out_path = os.path.join(base, 'frontend', 'public', 'assets', 'unt-logo-square.png')

    if not os.path.exists(img_path):
        print(f"File not found: {img_path}")
        sys.exit(1)

    img = QImage(img_path)
    if img.isNull():
        print("Failed to load image")
        sys.exit(1)

    w, h = img.width(), img.height()
    print(f"Original logo size: {w}x{h}")

    base_size = max(w, h)
    size = int(base_size * 1.35) # Add 35% padding for breathing room around the logo

    padded = QImage(size, size, QImage.Format.Format_ARGB32)
    padded.fill(QColor(0, 0, 0, 0))

    painter = QPainter(padded)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing)
    painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform)
    x = (size - w) // 2
    y = (size - h) // 2
    painter.drawImage(x, y, img)
    painter.end()

    success = padded.save(out_path)
    if success:
        print(f"Successfully generated squarified icon: {out_path}")
    else:
        print("Failed to save padded icon")
        sys.exit(1)

if __name__ == '__main__':
    pad_icon()
