# Fix apostrophes in app.py
content = open('app.py', encoding='utf-8').read()
# Replace curly apostrophe with straight apostrophe
content = content.replace('\u2019', "'")
open('app.py', 'w', encoding='utf-8').write(content)
print('Apostrofi corretti')
