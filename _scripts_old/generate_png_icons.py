#!/usr/bin/env python3
"""
Script per generare icone PNG per il manifest PWA.
Chrome su Android richiede icone PNG (non SVG) per mostrare il prompt di installazione.
"""

from PIL import Image, ImageDraw
import os

ICONS_DIR = os.path.join(os.path.dirname(__file__), 'static', 'icons')

# Colori dal manifest
BG_COLOR = (14, 165, 233)  # #0ea5e9
WHITE = (255, 255, 255)

SIZES = [72, 96, 128, 144, 152, 192, 384, 512]

def create_icon(size):
    """Crea un'icona PNG con il logo JobLog - J pulita e centrata."""
    img = Image.new('RGBA', (size, size), BG_COLOR)
    draw = ImageDraw.Draw(img)
    
    # Padding del 20% per safe area
    padding = size * 0.20
    
    # Area disponibile per il disegno
    x0 = padding
    y0 = padding
    w = size - (padding * 2)
    h = size - (padding * 2)
    
    # Spessore linea proporzionale (10% della larghezza disponibile)
    stroke = max(int(w * 0.10), 2)
    
    # === Disegno della J ===
    
    # Barra orizzontale superiore
    bar_y = y0 + h * 0.10
    bar_x1 = x0 + w * 0.20
    bar_x2 = x0 + w * 0.80
    draw.rounded_rectangle(
        [bar_x1, bar_y, bar_x2, bar_y + stroke],
        radius=stroke // 2,
        fill=WHITE
    )
    
    # Gambo verticale (centrato a destra)
    stem_x = x0 + w * 0.55
    stem_top = bar_y
    stem_bottom = y0 + h * 0.65
    draw.rectangle(
        [stem_x, stem_top, stem_x + stroke, stem_bottom],
        fill=WHITE
    )
    
    # Curva inferiore (arco)
    arc_radius = w * 0.25
    arc_center_x = stem_x - arc_radius + stroke / 2
    arc_center_y = stem_bottom
    
    # Bbox per l'arco
    arc_left = arc_center_x - arc_radius
    arc_top = arc_center_y - arc_radius
    arc_right = arc_center_x + arc_radius
    arc_bottom = arc_center_y + arc_radius
    
    draw.arc(
        [arc_left, arc_top, arc_right, arc_bottom],
        start=0,
        end=90,
        fill=WHITE,
        width=stroke
    )
    
    # Terminale (pallino alla fine della curva)
    terminal_x = arc_center_x - arc_radius
    terminal_y = arc_center_y
    terminal_r = stroke * 0.6
    draw.ellipse(
        [terminal_x - terminal_r, terminal_y - terminal_r,
         terminal_x + terminal_r, terminal_y + terminal_r],
        fill=WHITE
    )
    
    # Punto decorativo sopra la J
    dot_x = x0 + w * 0.30
    dot_y = y0 + h * 0.15
    dot_r = stroke * 0.8
    draw.ellipse(
        [dot_x - dot_r, dot_y - dot_r, dot_x + dot_r, dot_y + dot_r],
        fill=WHITE
    )
    
    # Salva
    output_path = os.path.join(ICONS_DIR, f'icon-{size}x{size}.png')
    img.save(output_path, 'PNG')
    print(f'✓ Creato: icon-{size}x{size}.png')
    return output_path

def main():
    print("Generazione icone PNG per PWA...")
    print(f"Directory: {ICONS_DIR}")
    print()
    
    if not os.path.exists(ICONS_DIR):
        os.makedirs(ICONS_DIR)
    
    for size in SIZES:
        create_icon(size)
    
    print()
    print("✅ Tutte le icone PNG sono state generate!")

if __name__ == '__main__':
    main()
