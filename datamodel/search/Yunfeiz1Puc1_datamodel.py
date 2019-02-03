'''
Created on Oct 20, 2016
@author: Rohan Achar
'''
from rtypes.pcc.attributes import dimension, primarykey, predicate
from rtypes.pcc.types.subset import subset
from rtypes.pcc.types.set import pcc_set
from rtypes.pcc.types.projection import projection
from rtypes.pcc.types.impure import impure
from datamodel.search.server_datamodel import Link, ServerCopy

@pcc_set
class Yunfeiz1Puc1Link(Link):
    USERAGENTSTRING = "Yunfeiz1Puc1"

    @dimension(str)
    def user_agent_string(self):
        return self.USERAGENTSTRING

    @user_agent_string.setter
    def user_agent_string(self, v):
        # TODO (rachar): Make it such that some dimensions do not need setters.
        pass


@subset(Yunfeiz1Puc1Link)
class Yunfeiz1Puc1UnprocessedLink(object):
    @predicate(Yunfeiz1Puc1Link.download_complete, Yunfeiz1Puc1Link.error_reason)
    def __predicate__(download_complete, error_reason):
        return not (download_complete or error_reason)


@impure
@subset(Yunfeiz1Puc1UnprocessedLink)
class OneYunfeiz1Puc1UnProcessedLink(Yunfeiz1Puc1Link):
    __limit__ = 1

    @predicate(Yunfeiz1Puc1Link.download_complete, Yunfeiz1Puc1Link.error_reason)
    def __predicate__(download_complete, error_reason):
        return not (download_complete or error_reason)

@projection(Yunfeiz1Puc1Link, Yunfeiz1Puc1Link.url, Yunfeiz1Puc1Link.download_complete)
class Yunfeiz1Puc1ProjectionLink(object):
    pass
