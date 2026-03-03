from mongo_store import MongoStore

def main():
    store = MongoStore()
    store.clear_collections()
    print("✅ Colecciones 'runs' y 'partials' vaciadas.")

if __name__ == "__main__":
    main()