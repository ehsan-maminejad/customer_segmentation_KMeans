def normalize_col(col):
    return (col-col.min()) / (col.max()-col.min())


def normalize_recency(col):
    return (col.max()-col) / (col.max()-col.min())
