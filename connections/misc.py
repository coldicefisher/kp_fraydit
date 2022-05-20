def read_from_pickle():
    import pickle
    objects = []

    with open('save_test1.pkl', 'rb') as f:
        try:
            while True:
                objects.append(pickle.load(f))
        except EOFError:
            pass