import pandas as pd
import utils.config as cfg



class Transform:
    def __init__(self):
        self.dollar_daily = pd.read_excel(cfg.root_path+'/components/category.csv')
