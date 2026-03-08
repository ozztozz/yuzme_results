"""
Gemini PDF Parser for Swimming Results
Uses Google's Gemini API to parse swimming competition PDFs and extract structured data.
"""

import json
import os
import re
from pathlib import Path
from typing import Any, Optional
import httpx
from dotenv import load_dotenv
from pypdf import PdfReader
import base64

# Load environment variables
load_dotenv()

# Get API Key
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    raise ValueError("GOOGLE_API_KEY not found in environment variables")


TAG_PATTERN = re.compile(r"\((?:Fd|Tk|Td)\)\s*", flags=re.IGNORECASE)


def _strip_fd_tk_td(value: str) -> str:
    """Remove (Fd), (Tk), (Td) markers from text fields."""
    return TAG_PATTERN.sub("", value).strip()


def _clean_csv_name_club(csv_data: str) -> str:
    """Clean marker tags from Name and Club columns in semicolon-separated CSV."""
    text = csv_data.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:csv)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text).strip()

    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return text

    cleaned_lines: list[str] = [lines[0]]
    for line in lines[1:]:
        parts = line.split(";")
        # Expected header:
        # EventTitle;EventDate;Gender;SwimmingStyle;Distance;Position;Name;BirthYear;Club;Time;Heat/Round;Points;Disqualification
        # Name is at index 6, Club is at index 8
        if len(parts) >= 9:
            parts[6] = _strip_fd_tk_td(parts[6])
            parts[8] = _strip_fd_tk_td(parts[8])
        cleaned_lines.append(";".join(parts))

    return "\n".join(cleaned_lines)


def _clean_json_name_club(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Clean marker tags from name/club fields in JSON output."""
    for item in items:
        if isinstance(item.get("name"), str):
            item["name"] = _strip_fd_tk_td(item["name"])
        if isinstance(item.get("club"), str):
            item["club"] = _strip_fd_tk_td(item["club"])
    return items


def parse_pdf_url_with_gemini(pdf_url: str, output_format: str = "csv") -> dict[str, Any]:
    """
    Parse PDF from URL using Gemini - downloads PDF and sends content to Gemini.
    
    Args:
        pdf_url: URL of the PDF file  
        output_format: "json" or "csv" format for output
    
    Returns:
        Dictionary containing parsed results
    """
    print(f"Processing PDF from URL: {pdf_url}")
    
    # Download the PDF
    print("Downloading PDF...")
    response = httpx.get(pdf_url, follow_redirects=True, timeout=30.0)
    response.raise_for_status()
    pdf_bytes = response.content
    pdf_base64 = base64.b64encode(pdf_bytes).decode('utf-8')
    print(f"Downloaded {len(pdf_bytes)} bytes")
    
    # Craft the prompt
    if output_format.lower() == "csv":
        prompt_text = """
This is a swimming competition results PDF. Extract all race results.

For each swimmer provide:
-Tittle of the event
-date of the event
-Gender (Erkek for male/boys, Kız for female/girls)
-swimming style in Turkish (e.g. Serbest, Sirtustu, Kelebek, Kurbagalama, Karisik)
-distance of the event (e.g. 50m, 100m, 200m)
- Position/Rank
- Swimmer Name
- Birth Year (or null if unavailable)
- Club/Team (or null if unavailable)
- Time
-heat/round information (if available)
- Points (if available)
- diqualification status (if available)

Important:
- Use Turkish event title (Etkinlik/Bras basligi Turkce olmali).
- Gender should be "Erkek" or "Kız" based on the event category.
- Remove (Fd), (Tk), (Td) markers from swimmer name and club name.

Return as CSV with semicolon delimiter.
Header: EventTitle;EventDate;Gender;SwimmingStyle;Distance;Position;Name;BirthYear;Club;Time;Heat/Round;Points;Disqualification

Provide ONLY the CSV data.
"""
    else:  # JSON format
        prompt_text = """
This is a swimming competition results PDF. Document language must be Turkish.
Extract all race results. Remove (Fd), (Tk), (Td) from names and club names.
Use Turkish title for event and swimming style.

For each swimmer provide:
- position (number)
- name (string)
- birth_year (string)
- gender (string: "Erkek" for male/boys, "Kız" for female/girls)
- gender (string or null)
- club (string)
- time (string)
- points (number or null)
- event_title (string)
- event_date (string)
- swimming_style (string)
- heat_round (string or null)
- disqualification (boolean or null)

Return as JSON array:
[{"position": 1, "name": "...", "birth_year": "...", "gender": "...", "club": "...", "time": "...", "points": ...}]

No markdown blocks, just the JSON array.
"""
    
    # Send to Gemini API with PDF data
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GOOGLE_API_KEY}"
    
    payload = {
        "contents": [{
            "parts": [
                {"text": prompt_text},
                {
                    "inline_data": {
                        "mime_type": "application/pdf",
                        "data": pdf_base64
                    }
                }
            ]
        }],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 65000
        }
    }
    
    print("Sending PDF content to Gemini API... (this may take 2-3 minutes)")
    response = httpx.post(api_url, json=payload, timeout=300.0)
    response.raise_for_status()
    
    result = response.json()
    
    # Extract generated text
    if "candidates" not in result or not result["candidates"]:
        raise ValueError("No response from Gemini API")
    
    result_text = result["candidates"][0]["content"]["parts"][0]["text"].strip()
    
    # Parse and return results
    if output_format.lower() == "csv":
        cleaned_csv = _clean_csv_name_club(result_text)
        return {
            "format": "csv",
            "data": cleaned_csv
        }
    else:
        # Clean JSON response
        cleaned_text = result_text
        if "```" in cleaned_text:
            import re
            match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', cleaned_text, re.DOTALL)
            if match:
                cleaned_text = match.group(1).strip()
            else:
                cleaned_text = cleaned_text.replace("```json", "").replace("```", "").strip()
        
        try:
            parsed_json = json.loads(cleaned_text)
            if isinstance(parsed_json, list):
                parsed_json = _clean_json_name_club(parsed_json)
            return {
                "format": "json",
                "data": parsed_json
            }
        except json.JSONDecodeError as e:
            print(f"Warning: Could not parse JSON: {e}")
            print(f"Response preview: {cleaned_text[:500]}")
            return {
                "format": "text",
                "data": cleaned_text,
                "error": f"Could not parse as JSON: {str(e)}"
            }


def download_pdf(url: str, save_path: Optional[str] = None) -> Path:
    """Download PDF from URL to temporary or specified location."""
    if save_path is None:
        save_path = "temp_downloaded.pdf"
    
    print(f"Downloading PDF from: {url}")
    response = httpx.get(url, follow_redirects=True, timeout=30.0)
    response.raise_for_status()
    
    pdf_path = Path(save_path)
    pdf_path.write_bytes(response.content)
    print(f"PDF saved to: {pdf_path}")
    return pdf_path


def parse_pdf_with_gemini(pdf_path: Path, output_format: str = "json") -> dict[str, Any]:
    """
    Parse PDF using Gemini API (via REST API) and extract swimming competition results.
    
    Args:
        pdf_path: Path to the PDF file
        output_format: "json" or "csv" format for output
    
    Returns:
        Dictionary containing parsed results
    """
    print(f"Parsing PDF with Gemini: {pdf_path}")
    
    # Read PDF as bytes and encode to base64
    pdf_bytes = pdf_path.read_bytes()
    pdf_base64 = base64.b64encode(pdf_bytes).decode('utf-8')
    
    # Craft the prompt based on output format
    if output_format.lower() == "csv":
        prompt_text = """
Analyze this swimming competition results PDF and extract all the race results.

For each swimmer/result, extract the following information:
- Position/Rank
- Swimmer Name
- Birth Year
- Club/Team
- Time
- Points (if available)
- Heat/Round information (if available)

Return the data in CSV format with a header row.
Use semicolon (;) as the delimiter.
Format: Position;Name;BirthYear;Club;Time;Points

Provide ONLY the CSV data, no other text or explanation.
"""
    else:  # JSON format
        prompt_text = """
Analyze this swimming competition results PDF and extract all the race results.

For each swimmer/result, extract the following information:
- position: The rank/position of the swimmer
- name: Full name of the swimmer
- birth_year: Year of birth
- club: Club or team name
- time: Race time (keep original format)
- points: Points scored (if available, otherwise null)
- heat: Heat or round number (if available, otherwise null)

Return the data as a JSON array of objects. Each object should represent one swimmer's result.

Example format:
[
  {
    "position": 1,
    "name": "SWIMMER NAME",
    "birth_year": "2005",
    "club": "CLUB NAME",
    "time": "01:23.45",
    "points": 500,
    "heat": 1
  }
]

IMPORTANT:
- Escape all special characters properly in JSON strings
- Complete the entire JSON array, don't truncate
- Provide ONLY valid JSON, no markdown code blocks or explanatory text
"""
    
    # Prepare the request to Gemini API
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GOOGLE_API_KEY}"
    
    payload = {
        "contents": [
            {
                "parts": [
                    {"text": prompt_text},
                    {
                        "inline_data": {
                            "mime_type": "application/pdf",
                            "data": pdf_base64
                        }
                    }
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0.1,
            "topK": 32,
            "topP": 0.8,
            "maxOutputTokens": 16384,
        }
    }
    
    print("Sending request to Gemini API...")
    response = httpx.post(api_url, json=payload, timeout=180.0)
    response.raise_for_status()
    
    result = response.json()
    
    # Extract the generated text
    if "candidates" not in result or not result["candidates"]:
        raise ValueError("No response from Gemini API")
    
    result_text = result["candidates"][0]["content"]["parts"][0]["text"].strip()
    
    # Parse and return results
    if output_format.lower() == "csv":
        return {
            "format": "csv",
            "data": result_text
        }
    else:
        # Try to extract JSON from the response
        # Remove markdown code blocks if present
        cleaned_text = result_text
        if "```json" in cleaned_text or "```" in cleaned_text:
            # Extract content between code blocks
            import re
            match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', cleaned_text, re.DOTALL)
            if match:
                cleaned_text = match.group(1).strip()
            else:
                # Remove just the markers
                cleaned_text = cleaned_text.replace("```json", "").replace("```", "").strip()
        
        try:
            parsed_json = json.loads(cleaned_text)
            return {
                "format": "json",
                "data": parsed_json
            }
        except json.JSONDecodeError as e:
            print(f"Warning: Could not parse JSON: {e}")
            print(f"First 500 chars of response: {cleaned_text[:500]}")
            return {
                "format": "text",
                "data": cleaned_text,
                "error": f"Could not parse as JSON: {str(e)}"
            }


def parse_pdf_from_url(url: str, output_format: str = "json", save_pdf: bool = False) -> dict[str, Any]:
    """
    Download and parse PDF from URL using Gemini API.
    
    Args:
        url: URL of the PDF file
        output_format: "json" or "csv"
        save_pdf: Whether to keep the downloaded PDF file
    
    Returns:
        Dictionary containing parsed results
    """
    pdf_path = download_pdf(url)
    
    try:
        results = parse_pdf_with_gemini(pdf_path, output_format)
        return results
    finally:
        if not save_pdf and pdf_path.exists():
            pdf_path.unlink()
            print(f"Cleaned up temporary PDF: {pdf_path}")


def save_results(results: dict[str, Any], output_file: str) -> None:
    """Save results to file."""
    output_path = Path(output_file)
    
    if results["format"] == "csv":
        output_path.write_text(results["data"], encoding="utf-8")
    else:
        output_path.write_text(json.dumps(results["data"], indent=2, ensure_ascii=False), encoding="utf-8")
    
    print(f"Results saved to: {output_path}")


if __name__ == "__main__":
    # Process PDF from URL - downloads and sends to Gemini
    PDF_URL = "https://canli.tyf.gov.tr/ankara/cs-1005146/canli/ResultList_4.pdf"
    
    print("="*60)
    print("Gemini PDF Parser - Swimming Results")
    print("Downloads PDF from URL and sends to Gemini")
    print("="*60)
    
    # Parse PDF from URL
    print("\n📄 Processing PDF from URL...")
    results = parse_pdf_url_with_gemini(PDF_URL, output_format="csv")
    
    # Save results
    save_results(results, "gemini_results.csv")
    
    print("\n✅ Done!")
    
    # Display results
    if results["format"] == "csv":
        lines = results["data"].split('\n')
        print(f"\n📊 Found {len(lines)-1} results (excluding header)")
        print(f"\nFirst 5 lines:")
        for line in lines[:5]:
            print(line)
    elif results["format"] == "json" and isinstance(results["data"], list):
        print(f"\n📊 Found {len(results['data'])} results")
        if results["data"]:
            print("\nFirst 3 results:")
            for i, result in enumerate(results["data"][:3], 1):
                print(f"{i}. {json.dumps(result, ensure_ascii=False)}")
    elif results.get("error"):
        print(f"\n⚠️ {results['error']}")
        print("Raw text saved to file.")
