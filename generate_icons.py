#!/usr/bin/env python3
"""
Genera PNG icons in tutte le dimensioni dal file SVG icon-192x192.svg
"""
import subprocess
import os
from pathlib import Path

# Percorso della cartella icons
ICONS_DIR = Path(__file__).parent / "static" / "icons"

# Dimensioni necessarie (da manifest.json)
SIZES = [72, 96, 128, 144, 152, 192, 384, 512]

# File SVG sorgente
SVG_SOURCE = ICONS_DIR / "icon-192x192.svg"

def generate_png(size):
    """Genera un PNG di una dimensione specifica dall'SVG"""
    output_file = ICONS_DIR / f"icon-{size}x{size}.png"
    
    # Usa ImageMagick (convert) per convertire SVG a PNG
    cmd = [
        "magick",
        f"{SVG_SOURCE}",
        "-resize", f"{size}x{size}",
        "-background", "none",
        f"{output_file}"
    ]
    
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        print(f"✓ Generato: {output_file}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Errore generando {size}x{size}: {e}")
        return False
    except FileNotFoundError:
        print("✗ ImageMagick non trovato. Prova con Pillow...")
        return generate_png_pillow(size)

def generate_png_pillow(size):
    """Genera PNG usando Pillow (PIL) se ImageMagick non è disponibile"""
    try:
        from PIL import Image
        import io
        
        # Leggi l'SVG e renderizzalo
        # Nota: Pillow non supporta nativamente SVG, serve un parser esterno
        # Per ora usiamo cairosvg se disponibile
        try:
            import cairosvg
            output_file = ICONS_DIR / f"icon-{size}x{size}.png"
            cairosvg.svg2png(
                url=str(SVG_SOURCE),
                write_to=str(output_file),
                output_width=size,
                output_height=size
            )
            print(f"✓ Generato (cairosvg): {output_file}")
            return True
        except ImportError:
            print("✗ cairosvg non trovato")
            return False
    except ImportError:
        print("✗ Pillow non trovato")
        return False

def main():
    print(f"Generando PNG icons da: {SVG_SOURCE}")
    print(f"Destinazione: {ICONS_DIR}\n")
    
    if not SVG_SOURCE.exists():
        print(f"✗ File SVG non trovato: {SVG_SOURCE}")
        return False
    
    success_count = 0
    for size in SIZES:
        if generate_png(size):
            success_count += 1
    
    print(f"\n✓ Completato: {success_count}/{len(SIZES)} PNG generati")
    return success_count == len(SIZES)

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
