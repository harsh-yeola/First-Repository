from PyPDF2 import PdfReader, PdfWriter

# Load the original PDF
input_path = "/mnt/data/Group_1_Dec_2023_Questions_and_Answers.pdf"
reader = PdfReader(input_path)

# Page ranges for each subject based on table of contents
sections = {
    "Jurisprudence_Interpretation_General_Laws.pdf": (3, 22),  # pages 1-20
    "Company_Law_Practice.pdf": (22, 51),  # pages 20-49
    "Setting_Up_Business_Industrial_Labour_Laws.pdf": (51, 73),  # pages 49-71
    "Corporate_Accounting_Financial_Management.pdf": (73, len(reader.pages)-1),  # pages 71-end
}

# Split and save PDFs
output_files = []
for filename, (start, end) in sections.items():
    writer = PdfWriter()
    for page_num in range(start, end):
        writer.add_page(reader.pages[page_num])
    output_path = f"/mnt/data/{filename}"
    with open(output_path, "wb") as f:
        writer.write(f)
    output_files.append(output_path)

output_files


#Result
#['/mnt/data/Jurisprudence_Interpretation_General_Laws.pdf',
# '/mnt/data/Company_Law_Practice.pdf',
# '/mnt/data/Setting_Up_Business_Industrial_Labour_Laws.pdf',
# '/mnt/data/Corporate_Accounting_Financial_Management.pdf']