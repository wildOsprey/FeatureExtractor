class AbstractLoader():
    def load_data(self, *args, **kwargs):
        raise NotImplementedError()

    def extract_features(self, *args, **kwargs):
        raise NotImplementedError()

    def export(self, *args, **kwargs):
        raise NotImplementedError()
