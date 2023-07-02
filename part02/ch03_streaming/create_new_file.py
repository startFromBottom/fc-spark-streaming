import time

if __name__ == "__main__":
    stocks_file_path = "data/stocks/"
    for i in range(10):
        file_name = f"{stocks_file_path}/{i}.csv"

        with open(file_name, "w") as file:
            data = "AAPL,2022.4.27,191.4910409\nAAPL,2022.4.28,200.1829065\nAAPL,2022.4.29,201.0920651"
            file.write(data)
            # time.sleep(5)
            print(f"{file_name} is written!")
