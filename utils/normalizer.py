class Normalizer:

    def normalize_lfm(self, col, col_max, col_min):
        return (col - col_min) / (col_max - col_min)

    def normalize_recency(self, col, col_max, col_min):
        return (col_max - col) / (col_max - col_min)
