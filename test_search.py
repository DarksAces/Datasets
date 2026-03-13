from googlesearch import search
try:
    for j in search("titanic dataset csv", num_results=5):
        print(j)
except Exception as e:
    print(f"Error: {e}")
