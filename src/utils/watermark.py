"""
Watermark utility for applying visual stamps to files.
Supports PDF, DOCX, and image files.
"""

import io
import structlog
from typing import Optional
from io import BytesIO

logger = structlog.get_logger(__name__)

WATERMARK_TEXT = "nxtworkforceai"


async def apply_watermark(file_bytes: bytes, file_name: str, watermark_id: Optional[str] = None) -> bytes:
    """
    Apply watermark to file bytes based on file type.
    
    Args:
        file_bytes: Original file bytes
        file_name: Original file name (for type detection)
        watermark_id: Optional ID to include in watermark text
        
    Returns:
        Watermarked file bytes, or original bytes if watermarking fails
    """
    if not file_bytes:
        return file_bytes
    
    file_name_lower = file_name.lower() if file_name else ""
    
    # Determine file type
    is_pdf = file_name_lower.endswith(".pdf") or "application/pdf" in file_name_lower
    is_docx = file_name_lower.endswith(".docx") or "wordprocessingml" in file_name_lower
    is_image = any(
        file_name_lower.endswith(ext) 
        for ext in [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".tiff", ".tif"]
    ) or any(mime in file_name_lower for mime in ["image/jpeg", "image/png", "image/gif"])
    
    try:
        if is_pdf:
            return await apply_pdf_watermark(file_bytes, watermark_id)
        elif is_docx:
            # DOCX watermarking is complex, skip for now or return original
            logger.warning("DOCX watermarking not yet implemented, skipping", file_name=file_name)
            return file_bytes
        elif is_image:
            return await apply_image_watermark(file_bytes, watermark_id)
        else:
            logger.debug("File type not supported for watermarking", file_name=file_name)
            return file_bytes
    except Exception as e:
        logger.error(
            "Failed to apply watermark",
            file_name=file_name,
            error=str(e),
            exc_info=True
        )
        # Return original bytes on error
        return file_bytes


async def apply_pdf_watermark(file_bytes: bytes, watermark_id: Optional[str] = None) -> bytes:
    """
    Apply watermark to PDF file.
    
    Args:
        file_bytes: PDF file bytes
        watermark_id: Optional ID to include in watermark text
        
    Returns:
        Watermarked PDF bytes
    """
    try:
        from pypdf import PdfReader, PdfWriter
        from reportlab.pdfgen import canvas
        from reportlab.lib.colors import grey
        
        # Create watermark text
        watermark_text = f"{WATERMARK_TEXT} - {watermark_id}" if watermark_id else WATERMARK_TEXT
        
        # Read original PDF
        pdf_reader = PdfReader(BytesIO(file_bytes))
        pdf_writer = PdfWriter()
        
        # Apply watermark to each page
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            
            # Get page dimensions
            page_width = float(page.mediabox.width)
            page_height = float(page.mediabox.height)
            
            # Create watermark for this page size
            watermark_page = BytesIO()
            c_page = canvas.Canvas(watermark_page, pagesize=(page_width, page_height))
            
            # Calculate font size (3% of smaller dimension)
            font_size = min(page_width, page_height) * 0.03
            c_page.setFont("Helvetica-Bold", font_size)
            c_page.setFillColor(grey, alpha=0.4)
            
            # Rotate and position watermark (centered, rotated -30 degrees)
            c_page.saveState()
            c_page.translate(page_width / 2, page_height / 2)
            c_page.rotate(-30)
            
            # Calculate text width for centering
            text_width = c_page.stringWidth(watermark_text, "Helvetica-Bold", font_size)
            c_page.drawString(-text_width / 2, 0, watermark_text)
            
            c_page.restoreState()
            c_page.save()
            watermark_page.seek(0)
            
            # Merge watermark with page
            watermark_reader = PdfReader(watermark_page)
            watermark_page_obj = watermark_reader.pages[0]
            page.merge_page(watermark_page_obj)
            
            pdf_writer.add_page(page)
        
        # Write watermarked PDF
        output = BytesIO()
        pdf_writer.write(output)
        output.seek(0)
        
        logger.info("Successfully applied PDF watermark", watermark_id=watermark_id)
        return output.getvalue()
                
    except ImportError as e:
        logger.warning("PDF watermarking libraries not available", error=str(e))
        return file_bytes
    except Exception as e:
        logger.error("Failed to apply PDF watermark", error=str(e), exc_info=True)
        return file_bytes


async def apply_image_watermark(file_bytes: bytes, watermark_id: Optional[str] = None) -> bytes:
    """
    Apply watermark to image file.
    
    Args:
        file_bytes: Image file bytes
        watermark_id: Optional ID to include in watermark text
        
    Returns:
        Watermarked image bytes
    """
    try:
        from PIL import Image, ImageDraw, ImageFont
        import math
        
        watermark_text = f"{WATERMARK_TEXT} - {watermark_id}" if watermark_id else WATERMARK_TEXT
        
        # Open image
        image = Image.open(BytesIO(file_bytes))
        
        # Get image dimensions
        width, height = image.size
        font_size = int(min(width, height) * 0.03)
        
        # Create drawing context
        draw = ImageDraw.Draw(image)
        
        # Try to use a font, fallback to default
        try:
            # Try to load a bold font
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", font_size)
        except:
            try:
                font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", font_size)
            except:
                # Fallback to default font
                font = ImageFont.load_default()
        
        # Get text dimensions
        bbox = draw.textbbox((0, 0), watermark_text, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        
        # Calculate position (center of image)
        x = (width - text_width) / 2
        y = (height - text_height) / 2
        
        # Create rotated text
        # Create a temporary image for the text
        text_img = Image.new("RGBA", (width, height), (255, 255, 255, 0))
        text_draw = ImageDraw.Draw(text_img)
        
        # Draw text with transparency
        text_draw.text((x, y), watermark_text, font=font, fill=(179, 179, 179, 102))  # 40% opacity
        
        # Rotate the text image
        rotated_text = text_img.rotate(-30, expand=False, fillcolor=(255, 255, 255, 0))
        
        # Composite the rotated text onto the original image
        image = Image.alpha_composite(image.convert("RGBA"), rotated_text).convert(image.mode)
        
        # Save to bytes
        output = BytesIO()
        image.save(output, format=image.format or "PNG")
        output.seek(0)
        
        logger.info("Successfully applied image watermark", watermark_id=watermark_id)
        return output.getvalue()
        
    except ImportError:
        logger.warning("PIL/Pillow not available for image watermarking")
        return file_bytes
    except Exception as e:
        logger.error("Failed to apply image watermark", error=str(e), exc_info=True)
        return file_bytes
