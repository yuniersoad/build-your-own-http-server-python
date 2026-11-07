from typing import BinaryIO, Optional

NO_COMPRESIR_SIGNATURES = {
    # Compresos clásicos
    b"\x1f\x8b": "application/gzip",
    b"PK\x03\x04": "application/zip",  # Incluye .zip, .docx, .xlsx, .jar
    b"BZh": "application/x-bzip2",
    b"\xfd7zXZ\x00": "application/x-xz",
    b"(\xb5/\xfd": "application/zstd",
    # Imágenes comunes
    b"\xff\xd8\xff": "image/jpeg",  # JPEG
    b"\x89PNG\r\n\x1a\n": "image/png",  # PNG
    b"GIF87a": "image/gif",  # GIF
    b"GIF89a": "image/gif",  # GIF
    # (Para WebP empieza con 'RIFF' y lleva 'WEBP' en el byte 8, pero con las comunes basta)
    b"RIFF": "image/webp / audio/wav",
    # Documentos / Otros
    b"%PDF": "application/pdf",  # PDF (ya usa compresión interna Deflate)
}


def is_already_compacted(fd: BinaryIO) -> bool:
    try:
        head = fd.read(12)
        for k, _ in NO_COMPRESIR_SIGNATURES.items():
            if head.startswith(k):
                return True
        
        return False
    except Exception:
        return False

def parse_http_range(range_header: str, file_size: int) -> Optional[tuple[int, int]]:
    """
    Parses an HTTP Range header (bytes=start-, bytes=start-end, bytes=-suffix)
    and returns an inclusive (start, end) tuple for reading the file,
    or None if unsatisfiable (should trigger a 416 status code).
    """
    if file_size <= 0:
        return None

    if not range_header.startswith("bytes="):
        return None

    spec = range_header[len("bytes="):]
    if "-" not in spec:
        return None

    start_str, end_str = spec.split("-", 1)

    try:
        # Format 3: bytes=-suffix-length (e.g., bytes=-500)
        if start_str == "" and end_str != "":
            suffix_length = int(end_str)
            if suffix_length <= 0:
                return None
            start = max(0, file_size - suffix_length)
            end = file_size - 1
            return (start, end)

        # Format 1: bytes=start- (e.g., bytes=500-)
        elif start_str != "" and end_str == "":
            start = int(start_str)
            if start >= file_size:
                return None  # Out of bounds -> 416
            end = file_size - 1
            return (start, end)

        # Format 2: bytes=start-end (e.g., bytes=0-499)
        elif start_str != "" and end_str != "":
            start = int(start_str)
            end = int(end_str)
            if start >= file_size or start > end:
                return None  # Invalid intersection -> 416
            
            # Clamp end if it exceeds the actual file size
            if end >= file_size:
                end = file_size - 1
            return (start, end)

        else:
            # Invalid format like "bytes=-"
            return None

    except ValueError:
        # Non-integer values encountered
        return None