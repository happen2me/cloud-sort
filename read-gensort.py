from pprint import pprint

records = []
with open("unsorted.dat", "rb") as f:
  r = f.read(100)
  while len(r) == 100:
    records.append(r)
    r = f.read(100)

s = sorted(records, key=lambda r: r[0:10])

print(len(records[0]))
print(type(records[0]))
print(len(records))

with open("sorted.dat", "wb") as f:
  for r in s:
    f.write(r)

