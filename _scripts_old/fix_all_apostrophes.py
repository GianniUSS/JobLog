# Fix all problematic apostrophes in app.py
import re

with open('app.py', 'rb') as f:
    content = f.read()

# Convert to string
text = content.decode('utf-8')

# Replace curly apostrophes (right single quotation mark U+2019) with straight apostrophe
count = text.count('\u2019')
print(f"Found {count} curly apostrophes to replace")

text = text.replace('\u2019', "'")

# Also check for left single quotation mark U+2018
count2 = text.count('\u2018')
if count2:
    print(f"Found {count2} left curly apostrophes to replace")
    text = text.replace('\u2018', "'")

# Write back
with open('app.py', 'w', encoding='utf-8') as f:
    f.write(text)

print("Fixed!")

# Verify
import ast
try:
    ast.parse(text)
    print("Syntax OK!")
except SyntaxError as e:
    print(f"Syntax error: {e}")
