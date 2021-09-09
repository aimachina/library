# pylint: disable=import-error
import io
import base64
from PIL import Image

# pylint: enable=import-error

DATA_URL_PREFIX = "data:image/jpeg;base64,"


def image_to_bytes(img):
    stream = io.BytesIO()
    img.save(stream, "JPEG")
    return stream.getvalue()


def image_to_base64(img, prefix=""):
    bytes_ = image_to_bytes(img) if type(img) is not bytes else img
    return prefix + str(base64.b64encode(bytes_), "utf-8")


def base64_to_image(bytes_):
    bytes_ = bytes_.split(",")[-1]  # Remove the dataurl prefix
    decoded = base64.b64decode(bytes_)
    return bytes_to_image(decoded)


def bytes_to_image(bytes_):
    img = Image.open(io.BytesIO(bytes_))
    img.verify()
    img = Image.open(io.BytesIO(bytes_)).convert("RGB")
    return img


def bytes_to_bytesIO(bytes_):
    img = Image.open(io.BytesIO(bytes_))
    img.verify()
    return io.BytesIO(bytes_)


def base64_to_bytesIO(bytes_):
    bytes_ = bytes_.split(",")[-1]  # Remove the dataurl prefix
    decoded = base64.b64decode(bytes_)
    return bytes_to_bytesIO(decoded)


def decode_image(src):
    if type(src) is bytes:
        return bytes_to_image(src)
    return base64_to_image(src)


def resize_image(image, max_dim=1800):
    w, h = image.size
    if w > h:
        ratio = max_dim / w
        w = max_dim
        h = int(h * ratio)
    else:
        ratio = max_dim / h
        h = max_dim
        w = int(w * ratio)
    return image.resize([w, h])


def thumbnail(image):
    w, h = image.size
    ratio = h / w
    new_w = max(80, int(w * 0.1))
    new_h = int(ratio * new_w)

    return image.resize((new_w, new_h))
