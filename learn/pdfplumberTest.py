import pdfplumber
#读取pdf数据
with pdfplumber.open("pdfs/11.pdf") as pdf:
    first_page = pdf.pages[0]
    print(first_page.chars[0])