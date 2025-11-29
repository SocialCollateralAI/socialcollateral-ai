from PIL import Image, ImageDraw, ImageFont
import os

def create_placeholder_image(text, filename, size=(400, 300), bg_color=(200, 200, 200), text_color=(80, 80, 80)):
    """Create a simple placeholder image with text"""
    # Create image
    img = Image.new('RGB', size, bg_color)
    draw = ImageDraw.Draw(img)
    
    # Try to use a default font
    try:
        font = ImageFont.truetype("arial.ttf", 24)
    except:
        try:
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 24)
        except:
            font = ImageFont.load_default()
    
    # Get text size and position for centering
    bbox = draw.textbbox((0, 0), text, font=font)
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]
    
    x = (size[0] - text_width) // 2
    y = (size[1] - text_height) // 2
    
    # Draw text
    draw.text((x, y), text, fill=text_color, font=font)
    
    # Save image
    img.save(filename, 'JPEG', quality=85)
    print(f"✅ Created: {filename}")

# Create directories if they don't exist
os.makedirs("data/images/home", exist_ok=True)
os.makedirs("data/images/bisnis", exist_ok=True)

# Create placeholder images
create_placeholder_image("PLACEHOLDER\nHOME IMAGE", "data/images/placeholder_home.jpg", bg_color=(220, 240, 220))
create_placeholder_image("PLACEHOLDER\nBUSINESS IMAGE", "data/images/placeholder_bisnis.jpg", bg_color=(240, 220, 220))

# Create some sample home images
home_samples = [
    "Sample Home 1", "Sample Home 2", "Sample Home 3", 
    "Sample Home 4", "Sample Home 5"
]

for i, text in enumerate(home_samples, 1):
    create_placeholder_image(f"HOME\n{text}", f"data/images/home/home_{i:02d}.jpg", bg_color=(200, 220, 180))

# Create some sample business images  
bisnis_samples = [
    "Warung Makan", "Toko Kelontong", "Bengkel Motor", 
    "Salon Kecantikan", "Laundry Service"
]

for i, text in enumerate(bisnis_samples, 1):
    create_placeholder_image(f"BISNIS\n{text}", f"data/images/bisnis/bisnis_{i:02d}.jpg", bg_color=(180, 200, 220))

print("\n🎉 All placeholder images created successfully!")
print("📁 Check data/images/ folder for the generated images")