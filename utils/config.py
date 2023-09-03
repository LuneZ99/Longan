from dot_dict import DotDict

global_config = DotDict.from_yaml('config.yaml')

if __name__ == '__main__':
    print(global_config)
