import jdatetime


def convert_date(date):
    j_date = jdatetime.datetime.strptime(date, '%Y/%m/%d').date()
    g_date = j_date.togregorian()
    return g_date
