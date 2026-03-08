from pathlib import Path
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions, EasyOcrOptions

# Configure OCR with Turkish language support
pipeline_options = PdfPipelineOptions()
pipeline_options.do_ocr = True
pipeline_options.images_scale = 2.0
pipeline_options.ocr_options = EasyOcrOptions(
    lang=["tr", "en"],
    force_full_page_ocr=True,
    bitmap_area_threshold=0.01,
    confidence_threshold=0.35,
)

source = "https://canli.tyf.gov.tr/ankara/cs-1005146/canli/ResultList_22.pdf"
converter = DocumentConverter(
    format_options={
        InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
    }
)
doc = converter.convert(source).document

# Save to txt file
output_path = Path("results/ResultList_22_text_docling.txt")
output_path.parent.mkdir(parents=True, exist_ok=True)
text = doc.export_to_markdown()
output_path.write_text(text, encoding="utf-8")
