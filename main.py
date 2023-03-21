from dataloader import DataLoader

if __name__ == '__main__':
    loader = DataLoader('./data')
    loader.load_data()
    loader.close()
