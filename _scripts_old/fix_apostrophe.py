with open('app.py', 'r', encoding='utf-8-sig') as f:
    content = f.read()

# Sostituisci apostrofi curvati con normali
content = content.replace('\u2019', "'")
content = content.replace('\u2018', "'")

with open('app.py', 'w', encoding='utf-8') as f:
    f.write(content)
print('Apostrofi e BOM corretti')
