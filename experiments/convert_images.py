import os
from PIL import Image
from pathlib import Path

def convert_webp_to_png(directory="."):
    """
    Converts all .webp files in the specified directory to .png.
    """
    # Resolve absolute path
    path = Path(directory).resolve()
    print(f"Scanning directory: {path}")

    # List files
    files = list(path.glob("*.webp"))
    
    if not files:
        print("No .webp files found.")
        return

    print(f"Found {len(files)} .webp files. Starting conversion...")

    for webp_file in files:
        try:
            # Create output filename
            png_file = webp_file.with_suffix(".png")
            
            # Open and save as PNG
            with Image.open(webp_file) as img:
                img.save(png_file, "PNG")
            
            print(f"Converted: {webp_file.name} -> {png_file.name}")
        except Exception as e:
            print(f"Error converting {webp_file.name}: {e}")

if __name__ == "__main__":
    # You can change the directory path here
    # target_dir = r"C:\path\to\your\images"
    target_dir = r"C:\Users\anjak\OneDrive\Desktop\img" 
    
    # Requirement Check
    try:
        import PIL
    except ImportError:
        print("Error: The 'Pillow' library is required.")
        print("Please install it using: pip install Pillow")
        os._exit(1)

    convert_webp_to_png(target_dir)
    print("\nConversion complete.")
