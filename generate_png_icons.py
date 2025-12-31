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
    """Crea un'icona PNG con il logo JobLog."""
    img = Image.new('RGBA', (size, size), BG_COLOR)
    draw = ImageDraw.Draw(img)
    
    # Calcoliamo le proporzioni per il logo "J"
    # Margine del 20%
    margin = int(size * 0.2)
    inner_size = size - (margin * 2)
    
    # Disegniamo una "J" stilizzata
    line_width = max(int(inner_size * 0.15), 3)
    
    # Barra orizzontale superiore della J
    bar_top = margin + int(inner_size * 0.1)
    bar_left = margin + int(inner_size * 0.2)
    bar_right = margin + int(inner_size * 0.8)
    draw.rectangle(
        [bar_left, bar_top, bar_right, bar_top + line_width],
        fill=WHITE
    )
    
    # Barra verticale della J
    vertical_left = margin + int(inner_size * 0.5)
    vertical_top = bar_top
    vertical_bottom = margin + int(inner_size * 0.75)
    draw.rectangle(
        [vertical_left, vertical_top, vertical_left + line_width, vertical_bottom],
        fill=WHITE
    )
    
    # Curva inferiore della J (semicerchio)
    curve_center_x = margin + int(inner_size * 0.35)
    curve_center_y = vertical_bottom
    curve_radius = int(inner_size * 0.15) + line_width // 2
    
    # Disegniamo un arco per la curva della J
    arc_left = curve_center_x - curve_radius
    arc_top = curve_center_y - curve_radius
    arc_right = curve_center_x + curve_radius
    arc_bottom = curve_center_y + curve_radius
    
    # Arco inferiore (180 a 270 gradi)
    draw.arc(
        [arc_left, arc_top, vertical_left + line_width, vertical_bottom + curve_radius],
        start=90,
        end=180,
        fill=WHITE,
        width=line_width
    )
    
    # Piccola linea per completare la curva
    draw.rectangle(
        [margin + int(inner_size * 0.2), vertical_bottom, 
         margin + int(inner_size * 0.35), vertical_bottom + line_width],
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
    print()
    print("Aggiorna manifest.json per usare le nuove icone PNG.")

if __name__ == '__main__':
    main()
