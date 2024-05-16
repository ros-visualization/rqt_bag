# The Python Imaging Library (PIL) is
#
#     Copyright © 1997-2011 by Secret Labs AB
#     Copyright © 1995-2011 by Fredrik Lundh
#
# Pillow is the friendly PIL fork. It is
#
#     Copyright © 2010-2024 by Jeffrey A. Clark (Alex) and contributors.
#
# Like PIL, Pillow is licensed under the open source HPND License:
#
# By obtaining, using, and/or copying this software and/or its associated
# documentation, you agree that you have read, understood, and will comply
# with the following terms and conditions:
#
# Permission to use, copy, modify and distribute this software and its
# documentation for any purpose and without fee is hereby granted,
# provided that the above copyright notice appears in all copies, and that
# both that copyright notice and this permission notice appear in supporting
# documentation, and that the name of Secret Labs AB or the author not be
# used in advertising or publicity pertaining to distribution of the software
# without specific, written prior permission.
#
# SECRET LABS AB AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS
# SOFTWARE, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS.
# IN NO EVENT SHALL SECRET LABS AB OR THE AUTHOR BE LIABLE FOR ANY SPECIAL,
# INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
# LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE
# OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
# PERFORMANCE OF THIS SOFTWARE.

# This code is adapted from the code in PIL, specifically
# https://github.com/python-pillow/Pillow/blob/10.2.0/src/PIL/ImageQt.py .
# We had to adapt it here to support PyQt5, which has been removed
# from upstream PIL but is still in use by rqt today.

from pathlib import Path

import PIL.Image as Image

from python_qt_binding.QtGui import QImage, QPixmap, qRgba

def rgb(r, g, b, a=255):
    """(Internal) Turns an RGB color into a Qt compatible color integer."""
    # use qRgb to pack the colors, and then turn the resulting long
    # into a negative integer with the same bitpattern.
    return qRgba(r, g, b, a) & 0xFFFFFFFF

def align8to32(bytes, width, mode):
    """
    converts each scanline of data from 8 bit to 32 bit aligned
    """

    bits_per_pixel = {"1": 1, "L": 8, "P": 8, "I;16": 16}[mode]

    # calculate bytes per line and the extra padding if needed
    bits_per_line = bits_per_pixel * width
    full_bytes_per_line, remaining_bits_per_line = divmod(bits_per_line, 8)
    bytes_per_line = full_bytes_per_line + (1 if remaining_bits_per_line else 0)

    extra_padding = -bytes_per_line % 4

    # already 32 bit aligned by luck
    if not extra_padding:
        return bytes

    new_data = [
        bytes[i * bytes_per_line : (i + 1) * bytes_per_line] + b"\x00" * extra_padding
        for i in range(len(bytes) // bytes_per_line)
    ]

    return b"".join(new_data)


def is_path(f):
    return isinstance(f, (bytes, str, Path))


def _toqclass_helper(im):
    data = None
    colortable = None
    exclusive_fp = False

    # handle filename, if given instead of image name
    if hasattr(im, "toUtf8"):
        # FIXME - is this really the best way to do this?
        im = str(im.toUtf8(), "utf-8")
    if is_path(im):
        im = Image.open(im)
        exclusive_fp = True

    qt_format = QImage
    if im.mode == "1":
        format = qt_format.Format_Mono
    elif im.mode == "L":
        format = qt_format.Format_Indexed8
        colortable = [rgb(i, i, i) for i in range(256)]
    elif im.mode == "P":
        format = qt_format.Format_Indexed8
        palette = im.getpalette()
        colortable = [rgb(*palette[i : i + 3]) for i in range(0, len(palette), 3)]
    elif im.mode == "RGB":
        # Populate the 4th channel with 255
        im = im.convert("RGBA")

        data = im.tobytes("raw", "BGRA")
        format = qt_format.Format_RGB32
    elif im.mode == "RGBA":
        data = im.tobytes("raw", "BGRA")
        format = qt_format.Format_ARGB32
    elif im.mode == "I;16" and hasattr(qt_format, "Format_Grayscale16"):  # Qt 5.13+
        im = im.point(lambda i: i * 256)

        format = qt_format.Format_Grayscale16
    else:
        if exclusive_fp:
            im.close()
        msg = f"unsupported image mode {repr(im.mode)}"
        raise ValueError(msg)

    size = im.size
    __data = data or align8to32(im.tobytes(), size[0], im.mode)
    if exclusive_fp:
        im.close()
    return {"data": __data, "size": size, "format": format, "colortable": colortable}


class ImageQt(QImage):
    def __init__(self, im):
        """
        An PIL image wrapper for Qt.  This is a subclass of PyQt's QImage
        class.

        :param im: A PIL Image object, or a file name (given either as
            Python string or a PyQt string object).
        """
        im_data = _toqclass_helper(im)
        # must keep a reference, or Qt will crash!
        # All QImage constructors that take data operate on an existing
        # buffer, so this buffer has to hang on for the life of the image.
        # Fixes https://github.com/python-pillow/Pillow/issues/1370
        self.__data = im_data["data"]
        super().__init__(
            self.__data,
            im_data["size"][0],
            im_data["size"][1],
            im_data["format"],
        )
        if im_data["colortable"]:
            self.setColorTable(im_data["colortable"])
