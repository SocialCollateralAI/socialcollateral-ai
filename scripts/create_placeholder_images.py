#!/usr/bin/env python3
"""
Script to create placeholder images for home and business
"""
import os
from PIL import Image, ImageDraw, ImageFont

def create_placeholder_image(width, height, text, color, filename):
    """Create a simple placeholder image with text"""
    # Create image with background color
    img = Image.new('RGB', (width, height), color)
    draw = ImageDraw.Draw(img)
    
    # Try to use a font, fallback to default if not available
    try:
        font = ImageFont.truetype("arial.ttf", 40)
    except:
        try:
            font = ImageFont.load_default()
        except:
            font = None
    
    # Calculate text position to center it
    if font:
        bbox = draw.textbbox((0, 0), text, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
    else:
        text_width = len(text) * 10  # Rough estimate
        text_height = 20
    
    x = (width - text_width) // 2
    y = (height - text_height) // 2
    
    # Draw text
    draw.text((x, y), text, fill='white', font=font)
    
    # Save image
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    img.save(filename, 'JPEG', quality=85)
    print(f"Created: {filename}")

# Create directories
os.makedirs("data/images/home", exist_ok=True)
os.makedirs("data/images/bisnis", exist_ok=True)

# Create placeholder images
create_placeholder_image(400, 300, "HOME\nPLACEHOLDER", (70, 130, 180), "data/images/placeholder_home.jpg")
create_placeholder_image(400, 300, "BUSINESS\nPLACEHOLDER", (255, 140, 0), "data/images/placeholder_bisnis.jpg")

# Create some sample home images
home_samples = [
    ("RUMAH\nTYPE 36", (34, 139, 34)),
    ("RUMAH\nTRADISIONAL", (46, 125, 50)),
    ("RUMAH\nMODERN", (67, 160, 71)),
    ("RUMAH\nSEDERHANA", (102, 187, 106)),
]

for i, (text, color) in enumerate(home_samples, 1):
    create_placeholder_image(400, 300, text, color, f"data/images/home/sample_home_{i:02d}.jpg")

# Create some sample business images  
bisnis_samples = [
    ("WARUNG\nMAKAN", (255, 152, 0)),
    ("TOKO\nKELONTONG", (255, 167, 38)),
    ("SALON\nKECANTIKAN", (255, 183, 77)),
    ("BENGKEL\nMOTOR", (255, 193, 7)),
]

for i, (text, color) in enumerate(bisnis_samples, 1):
    create_placeholder_image(400, 300, text, color, f"data/images/bisnis/sample_bisnis_{i:02d}.jpg")

print("\n✅ All placeholder images created successfully!")
print("📁 Structure:")
print("data/images/")
print("├── placeholder_home.jpg")
print("├── placeholder_bisnis.jpg") 
print("├── home/")
print("│   ├── sample_home_01.jpg")
print("│   ├── sample_home_02.jpg")
print("│   ├── sample_home_03.jpg")
print("│   └── sample_home_04.jpg")
print("└── bisnis/")
print("    ├── sample_bisnis_01.jpg")
print("    ├── sample_bisnis_02.jpg")
print("    ├── sample_bisnis_03.jpg")
print("    └── sample_bisnis_04.jpg")