def find_word_chain(word_list, start_word):
    tmp = sorted(word_list, key=lambda x: (-len(x), x))

    def can_chain(word, next_word):
        return word[-1] == next_word[0]

    def dfs(cur):
        last = cur[-1]
        max = cur

        for i in tmp:
            if i not in cur and can_chain(last, i):
                new = dfs(cur + [i])
                if len(new)> len(max):
                    max = new
        return max
    start_chain = [start_word]
    long = dfs(start_chain)
    return ''.join(long)
