from .analytics_rule import AnalyticsRule


class AnalyticsRules(object):
    RESOURCE_PATH = '/analytics/rules'

    def __init__(self, api_call):
        self.api_call = api_call
        self.rules = {}

    def __getitem__(self, rule_id):
        if rule_id not in self.rules:
            self.rules[rule_id] = AnalyticsRule(self.api_call, rule_id)

        return self.rules[rule_id]

    async def create(self, rule, params=None):
        params = params or {}
        return await self.api_call.post(AnalyticsRules.RESOURCE_PATH, rule, params)

    async def upsert(self, id, rule):
        return await self.api_call.put(u"{0}/{1}".format(AnalyticsRules.RESOURCE_PATH, id), rule)

    async def retrieve(self):
        return await self.api_call.get(AnalyticsRules.RESOURCE_PATH)

