# Fix BOM and apostrophes in app.py
with open('app.py', 'rb') as f:
    content = f.read()

# Remove BOM if present
if content.startswith(b'\xef\xbb\xbf'):
    print("Removing BOM...")
    content = content[3:]

# Convert to string
text = content.decode('utf-8')

# Replace curly apostrophes
count1 = text.count('\u2019')  # right
count2 = text.count('\u2018')  # left
print(f"Found {count1} right curly apostrophes, {count2} left curly apostrophes")

text = text.replace('\u2019', "'").replace('\u2018', "'")

# Write back WITHOUT BOM
with open('app.py', 'w', encoding='utf-8', newline='\n') as f:
    f.write(text)

print("Fixed!")

# Verify syntax
import ast
try:
    ast.parse(text)
    print("Syntax OK!")
except SyntaxError as e:
    print(f"Syntax error remains: {e}")
