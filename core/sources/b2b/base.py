from core.sources.base import Source


class B2BSource(Source):
    CONNECTOR = None

    def parse_data(self, connector, **kwargs):
        raise NotImplementedError

    def process(self, **kwargs):
        args = self.set_arguments(**kwargs)
        conn = self.CONNECTOR.get_instance(**args)
        return self.parse_data(conn, **kwargs)


class B2BWebSource(B2BSource):
    def set_arguments(self, **kwargs):
        initial_day = self.actual_date
        args = {
            'date_start': initial_day,
            'b2b_username': kwargs['b2b_username'],
            'b2b_password': kwargs['b2b_password']
        }
        return args


class B2BFileSource(B2BSource):
    def set_arguments(self, **kwargs):
        initial_day = self.actual_date
        args = {
            'date_start': initial_day,
            'b2b_username': kwargs['b2b_username'],
        }
        return args


class B2BPortalSource(B2BSource):
    def set_arguments(self, **kwargs):
        args = {
            'b2b_username': kwargs['b2b_username'],
            'b2b_password': kwargs['b2b_password']
        }
        return args
