import zipfile
import tarfile
import os
import hashlib


def extract_archive(filename, path=None):
    path = path or os.path.dirname(filename)
    if tarfile.is_tarfile(filename):
        with tarfile.TarFile(filename) as tf:
            tf.extractall(path)
        # Extract tarball
    elif zipfile.is_zipfile(filename):
        with zipfile.ZipFile(filename) as zf:
            zf.extractall(path)
    else:
        raise ValueError("Archive format is not supported.")


def remove_dir(top):
    for root, dirs, files in os.walk(top, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))
    os.rmdir(top)

def compute_signature(data: bytes):
    return hashlib.sha256(data).hexdigest()

IMAGE_EXTENSIONS = {"jpg", "png", "jpeg", "heif"}
DOCUMENT_EXTENSIONS = {"xls", "xlsx", "csv", "txt", "xlsb", "doc", "docx", "pdf"}
ARCHIVE_EXTENSIONS = {"zip", "rar"}
EMAIL_EXTENSIONS = {"msg", "eml"}
ALLOWED_EXTENSIONS = IMAGE_EXTENSIONS.union(ARCHIVE_EXTENSIONS).union(DOCUMENT_EXTENSIONS).union(EMAIL_EXTENSIONS)

def extension(filename):
    return filename.rsplit(".", 1)[-1].lower()

def is_image(filename, image_extensions=IMAGE_EXTENSIONS):
    return extension(filename) in image_extensions

def is_document(filename, document_extensions=DOCUMENT_EXTENSIONS):
    return extension(filename) in document_extensions

def is_archive(filename, archive_extensions=ARCHIVE_EXTENSIONS):
    return extension(filename) in archive_extensions

def is_email(filename, email_extensions=EMAIL_EXTENSIONS):
    return extension(filename) in email_extensions

def allowed_file(filename, allowed_extensions=ALLOWED_EXTENSIONS):
    return "." in filename and extension(filename) in allowed_extensions

def guess_filetype(filename):
    if is_image(filename):
        return 'img'
    elif is_document(filename):
        return 'txt'
    elif is_archive(filename):
        return 'archive'
    elif is_email(filename):
        return 'email'
