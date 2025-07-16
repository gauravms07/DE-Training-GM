square = [x**2 for x in range(10)]
evens = [x for x in range(10) if(x%2==0)]

print(f"square comp {square}")
print(f"evens comp {evens}")

square2 = list(map(lambda x : x**2, range(10)))
evens2 = list(filter(lambda x:x%2==0, range(10)))

print(f"square map {square2}")
print(f"evens filter {evens2}")
               