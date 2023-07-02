from collections import defaultdict

if __name__ == "__main__":

    words_count: dict[str, int] = defaultdict(int)

    with open("data/words.txt", "r") as file:

        for _, line in enumerate(file):
            words_each_line = line.strip().split(" ")

            for _, word in enumerate(words_each_line):
                words_count[word] += 1

    for word, count in words_count.items():
        print(f"{word}: {count}")
