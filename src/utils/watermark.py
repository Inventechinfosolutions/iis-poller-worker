"""
Watermark utility for applying visual stamps to files.
Supports PDF, DOCX, and image files.
"""

import io
import structlog
import zipfile
import re
from typing import Optional
from io import BytesIO

logger = structlog.get_logger(__name__)

WATERMARK_TEXT = "nxtworkforceai"


def escape_xml(text: str) -> str:
    """Escape XML special characters."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


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
    ) or any(mime in file_name_lower for mime in ["image/jpeg", "image/png", "image/gif", "image/webp", "image/tiff", "image/bmp"])
    
    try:
        if is_pdf:
            return await apply_pdf_watermark(file_bytes, watermark_id)
        elif is_docx:
            return await apply_docx_watermark(file_bytes, watermark_id)
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
        
        watermark_text = f"{WATERMARK_TEXT} - {watermark_id}" if watermark_id else WATERMARK_TEXT
        
        # Open image and preserve format
        image = Image.open(BytesIO(file_bytes))
        original_format = image.format or "PNG"
        original_mode = image.mode
        
        # Get image dimensions
        width, height = image.size
        font_size = max(int(min(width, height) * 0.03), 12)  # Minimum 12px font
        
        # Convert to RGBA for watermarking (supports transparency)
        if image.mode != "RGBA":
            image = image.convert("RGBA")
        
        # Try to use a font, fallback to default
        try:
            # Try to load a bold font
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", font_size)
        except:
            try:
                font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", font_size)
            except:
                try:
                    # Try default system font paths
                    font = ImageFont.truetype("/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf", font_size)
                except:
                    # Fallback to default font
                    font = ImageFont.load_default()
        
        # Create a temporary image for the watermark text
        text_img = Image.new("RGBA", (width, height), (255, 255, 255, 0))
        text_draw = ImageDraw.Draw(text_img)
        
        # Get text dimensions
        bbox = text_draw.textbbox((0, 0), watermark_text, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        
        # Calculate position (center of image)
        x = (width - text_width) / 2
        y = (height - text_height) / 2
        
        # Draw text with transparency (40% opacity = 102/255)
        text_draw.text((x, y), watermark_text, font=font, fill=(179, 179, 179, 102))
        
        # Rotate the text image -30 degrees
        rotated_text = text_img.rotate(-30, expand=False, fillcolor=(255, 255, 255, 0))
        
        # Composite the rotated text onto the original image
        watermarked_image = Image.alpha_composite(image, rotated_text)
        
        # Save to bytes preserving original format
        output = BytesIO()
        
        # Preserve original format
        if original_format.upper() in ["JPEG", "JPG"]:
            # Convert RGBA to RGB for JPEG (JPEG doesn't support transparency)
            if watermarked_image.mode == "RGBA":
                rgb_image = Image.new("RGB", watermarked_image.size, (255, 255, 255))
                rgb_image.paste(watermarked_image, mask=watermarked_image.split()[3])  # Use alpha channel as mask
                watermarked_image = rgb_image
            watermarked_image.save(output, format="JPEG", quality=90)
        elif original_format.upper() == "PNG":
            watermarked_image.save(output, format="PNG")
        elif original_format.upper() == "WEBP":
            watermarked_image.save(output, format="WEBP", quality=90)
        elif original_format.upper() in ["TIFF", "TIF"]:
            watermarked_image.save(output, format="TIFF")
        elif original_format.upper() == "GIF":
            # GIF handling - convert to RGB if needed
            if watermarked_image.mode == "RGBA":
                rgb_image = Image.new("RGB", watermarked_image.size, (255, 255, 255))
                rgb_image.paste(watermarked_image, mask=watermarked_image.split()[3])
                watermarked_image = rgb_image
            watermarked_image.save(output, format="GIF")
        elif original_format.upper() == "BMP":
            # BMP handling - convert to RGB if needed
            if watermarked_image.mode == "RGBA":
                rgb_image = Image.new("RGB", watermarked_image.size, (255, 255, 255))
                rgb_image.paste(watermarked_image, mask=watermarked_image.split()[3])
                watermarked_image = rgb_image
            watermarked_image.save(output, format="BMP")
        else:
            # Default to PNG for other formats
            watermarked_image.save(output, format="PNG")
        
        output.seek(0)
        
        logger.info(
            "Successfully applied image watermark",
            watermark_id=watermark_id,
            format=original_format,
            original_mode=original_mode
        )
        return output.getvalue()
        
    except ImportError:
        logger.warning("PIL/Pillow not available for image watermarking")
        return file_bytes
    except Exception as e:
        logger.error("Failed to apply image watermark", error=str(e), exc_info=True)
        return file_bytes


async def apply_docx_watermark(file_bytes: bytes, watermark_id: Optional[str] = None) -> bytes:
    """
    Apply watermark to DOCX file.
    
    Args:
        file_bytes: DOCX file bytes
        watermark_id: Optional ID to include in watermark text
        
    Returns:
        Watermarked DOCX bytes
    """
    try:
        watermark_text = f"{WATERMARK_TEXT} - {watermark_id}" if watermark_id else WATERMARK_TEXT
        escaped_text = escape_xml(watermark_text)
        
        logger.info("Starting DOCX watermarking", watermark_text=watermark_text, watermark_id=watermark_id)
        
        # DOCX files are ZIP archives
        docx_zip = zipfile.ZipFile(BytesIO(file_bytes), 'r')
        
        # Read document.xml
        document_xml = docx_zip.read('word/document.xml').decode('utf-8')
        logger.debug("Read document.xml", length=len(document_xml))
        
        # Create watermark VML (Vector Markup Language) for Word
        watermark_vml = f'''
      <w:p>
        <w:pPr>
          <w:spacing w:before="0" w:after="0"/>
        </w:pPr>
        <w:r>
          <w:pict>
            <v:shapetype id="_x0000_t202"
              coordsize="21600,21600"
              o:spt="202">
              <v:stroke joinstyle="miter"/>
              <v:path o:connecttype="none"/>
            </v:shapetype>
      
            <v:shape id="WaterMarkObject"
              type="#_x0000_t202"
              style="position:absolute;
                     margin-left:0;
                     margin-top:0;
                     width:600pt;
                     height:100pt;
                     rotation:-30;
                     mso-position-horizontal:center;
                     mso-position-horizontal-relative:page;
                     mso-position-vertical:center;
                     mso-position-vertical-relative:page;
                     opacity:0.4"
              fillcolor="none"
              stroked="false"
              opacity="0.4">
      
              <v:textbox inset="0,0,0,0">
                <w:txbxContent>
                  <w:p>
                    <w:pPr><w:jc w:val="center"/></w:pPr>
                    <w:r>
                      <w:rPr>
                        <w:color w:val="B3B3B3"/>
                        <w:sz w:val="24"/>
                      </w:rPr>
                      <w:t>{escaped_text}</w:t>
                    </w:r>
                  </w:p>
                </w:txbxContent>
              </v:textbox>
      
            </v:shape>
          </w:pict>
        </w:r>
      </w:p>'''
        
        # Check if header1.xml exists and find available relationship ID
        header_xml = None
        header_relationship_id = None
        
        # Read relationships to find available ID
        try:
            rels_xml = docx_zip.read('word/_rels/document.xml.rels').decode('utf-8')
            # Find all existing relationship IDs
            existing_ids = re.findall(r'Id="([^"]+)"', rels_xml)
            # Find next available ID (rId1, rId2, etc.)
            for i in range(1, 100):
                candidate_id = f'rId{i}'
                if candidate_id not in existing_ids:
                    header_relationship_id = candidate_id
                    break
            if not header_relationship_id:
                header_relationship_id = 'rId1'  # Fallback
        except KeyError:
            # No relationships file, use rId1
            header_relationship_id = 'rId1'
        
        # Check if header already exists
        try:
            header_xml = docx_zip.read('word/header1.xml').decode('utf-8')
            logger.debug("Found existing header1.xml")
        except KeyError:
            # Header doesn't exist, create it
            header_xml = None
            logger.debug("Header1.xml does not exist, will create new")
        
        # Create new ZIP in memory
        output_zip = BytesIO()
        new_zip = zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED)
        
        # Copy all files from original ZIP
        for item in docx_zip.infolist():
            if item.filename not in ['word/header1.xml', 'word/document.xml', 'word/_rels/document.xml.rels']:
                new_zip.writestr(item, docx_zip.read(item.filename))
        
        # Handle header
        if header_xml is None:
            # Create new header
            logger.debug("Creating new header1.xml")
            header_xml = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:hdr xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"
      xmlns:v="urn:schemas-microsoft-com:vml"
      xmlns:o="urn:schemas-microsoft-com:office:office"
      xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  {watermark_vml}
</w:hdr>'''
        else:
            # Add watermark to existing header if not already present
            logger.debug("Header exists, checking for watermark", has_watermark='WaterMarkObject' in header_xml)
            if 'WaterMarkObject' not in header_xml:
                # Find the header body content and insert watermark
                header_body_match = re.search(r'<w:hdr[^>]*>([\s\S]*)</w:hdr>', header_xml)
                if header_body_match:
                    header_content = header_body_match.group(1)
                    # Insert watermark at the beginning of header content
                    modified_header = header_xml.replace(
                        header_content,
                        f'{watermark_vml}{header_content}',
                        1
                    )
                    header_xml = modified_header
                    logger.debug("Added watermark to existing header")
                else:
                    # If no body found, insert after opening tag
                    header_xml = re.sub(
                        r'(<w:hdr[^>]*>)',
                        f'\\1{watermark_vml}',
                        header_xml,
                        count=1
                    )
                    logger.debug("Added watermark to header after opening tag")
            else:
                logger.debug("Watermark already exists in header, skipping")
        
        new_zip.writestr('word/header1.xml', header_xml.encode('utf-8'))
        logger.info("Created/updated header1.xml with watermark", 
                   header_length=len(header_xml),
                   has_watermark='WaterMarkObject' in header_xml)
        
        # Update document.xml to include header reference in ALL sections
        # This ensures watermark appears on all pages
        header_ref = f'<w:headerReference w:type="default" r:id="{header_relationship_id}"/>'
        
        # Find all sectPr elements (there may be multiple sections in the document)
        # Use a pattern that matches the entire sectPr block
        sect_pr_pattern = r'<w:sectPr([^>]*)>([\s\S]*?)</w:sectPr>'
        
        def add_header_ref(match):
            """Add header reference to a sectPr if it doesn't already have one."""
            sect_pr_attrs = match.group(1)
            sect_pr_content = match.group(2)
            
            # Check if header reference already exists
            if 'headerReference' not in match.group(0):
                # Insert header reference after opening sectPr tag
                return f'<w:sectPr{sect_pr_attrs}>{header_ref}{sect_pr_content}</w:sectPr>'
            else:
                # Already has header reference, return as-is
                return match.group(0)
        
        # Replace all sectPr elements
        sections_updated = len(re.findall(sect_pr_pattern, document_xml))
        document_xml = re.sub(sect_pr_pattern, add_header_ref, document_xml)
        logger.info("Updated document sections", sections_count=sections_updated, 
                   relationship_id=header_relationship_id)
        
        # If no sectPr was found, add one with header reference
        if '<w:sectPr' not in document_xml:
            logger.debug("No sectPr found, adding new one with header reference")
            header_sect_pr = f'''<w:sectPr>
              <w:headerReference w:type="default" r:id="{header_relationship_id}"/>
              <w:pgSz w:w="12240" w:h="15840"/>
              <w:pgMar w:top="1440" w:right="1440" w:bottom="1440" w:left="1440" w:header="708" w:footer="708" w:gutter="0"/>
              <w:cols w:space="708"/>
            </w:sectPr>'''
            document_xml = document_xml.replace('</w:body>', f'{header_sect_pr}</w:body>')
        
        new_zip.writestr('word/document.xml', document_xml.encode('utf-8'))
        
        # Update document relationships
        try:
            rels_xml = docx_zip.read('word/_rels/document.xml.rels').decode('utf-8')
            # Check if header1.xml relationship already exists
            if 'header1.xml' not in rels_xml:
                relationship = f'<Relationship Id="{header_relationship_id}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/header" Target="header1.xml"/>'
                rels_xml = rels_xml.replace('</Relationships>', f'{relationship}</Relationships>')
                logger.info("Added header relationship", relationship_id=header_relationship_id)
            else:
                logger.info("Header relationship already exists in document.xml.rels")
        except KeyError:
            # Create relationships file if it doesn't exist
            rels_xml = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="{header_relationship_id}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/header" Target="header1.xml"/>
</Relationships>'''
            logger.debug("Created new relationships file", relationship_id=header_relationship_id)
        
        new_zip.writestr('word/_rels/document.xml.rels', rels_xml.encode('utf-8'))
        
        # Close ZIP files
        docx_zip.close()
        new_zip.close()
        
        output_zip.seek(0)
        
        logger.info("Successfully applied DOCX watermark", watermark_id=watermark_id)
        return output_zip.getvalue()
        
    except Exception as e:
        logger.error("Failed to apply DOCX watermark", error=str(e), exc_info=True)
        return file_bytes
