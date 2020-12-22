# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import math
import re

from typing import Union, Dict, List
from nebula2.common.ttypes import (
    DataSet,
    Edge,
    Path,
    Row,
    Value,
    Vertex,
)
from tests.common.dataset_printer import DataSetPrinter

KV = Dict[Union[str, bytes], Value]
Pattern = type(re.compile(r'/'))


class DataSetComparator:
    def __init__(self,
                 strict=True,
                 order=False,
                 included=False,
                 decode_type: str = 'utf-8'):
        self._strict = strict
        self._order = order
        self._included = included
        self._decode_type = decode_type

    def __call__(self, resp: DataSet, expect: DataSet):
        return self.compare(resp, expect)

    def b(self, v: str) -> bytes:
        return v.encode(self._decode_type)

    def s(self, b: bytes) -> str:
        return b.decode(self._decode_type)

    def compare(self, resp: DataSet, expect: DataSet):
        if all(x is None for x in [expect, resp]):
            return True
        if None in [expect, resp]:
            return False
        if len(resp.rows) < len(expect.rows):
            return False
        if len(resp.column_names) != len(expect.column_names):
            return False
        for (ln, rn) in zip(resp.column_names, expect.column_names):
            if ln != self.bstr(rn):
                return False
        if self._order:
            return all(
                self.compare_row(l, r)
                for (l, r) in zip(resp.rows, expect.rows))
        return self._compare_list(resp.rows, expect.rows, self.compare_row,
                                  self._included)

    def compare_value(self, lhs: Value, rhs: Union[Value, Pattern]) -> bool:
        """
        lhs and rhs represent response data and expected data respectively
        """
        if type(rhs) is Pattern:
            dsp = DataSetPrinter(self._decode_type)
            return bool(rhs.match(dsp.to_string(lhs)))
        if lhs.getType() == Value.__EMPTY__:
            return rhs.getType() == Value.__EMPTY__
        if lhs.getType() == Value.NVAL:
            if not rhs.getType() == Value.NVAL:
                return False
            return lhs.get_nVal() == rhs.get_nVal()
        if lhs.getType() == Value.BVAL:
            if not rhs.getType() == Value.BVAL:
                return False
            return lhs.get_bVal() == rhs.get_bVal()
        if lhs.getType() == Value.IVAL:
            if not rhs.getType() == Value.IVAL:
                return False
            return lhs.get_iVal() == rhs.get_iVal()
        if lhs.getType() == Value.FVAL:
            if not rhs.getType() == Value.FVAL:
                return False
            return math.fabs(lhs.get_fVal() - rhs.get_fVal()) < 1.0E-8
        if lhs.getType() == Value.SVAL:
            if not rhs.getType() == Value.SVAL:
                return False
            return lhs.get_sVal() == self.bstr(rhs.get_sVal())
        if lhs.getType() == Value.DVAL:
            if rhs.getType() == Value.DVAL:
                return lhs.get_dVal() == rhs.get_dVal()
            if rhs.getType() == Value.SVAL:
                ld = lhs.get_dVal()
                lds = "%d-%02d-%02d" % (ld.year, ld.month, ld.day)
                rv = rhs.get_sVal()
                return lds == rv if type(rv) == str else self.b(lds) == rv
            return False
        if lhs.getType() == Value.TVAL:
            if rhs.getType() == Value.TVAL:
                return lhs.get_tVal() == rhs.get_tVal()
            if rhs.getType() == Value.SVAL:
                lt = lhs.get_tVal()
                lts = "%02d:%02d:%02d.%06d" % (lt.hour, lt.minute, lt.sec,
                                               lt.microsec)
                rv = rhs.get_sVal()
                return lts == rv if type(rv) == str else self.b(lts) == rv
            return False
        if lhs.getType() == Value.DTVAL:
            if rhs.getType() == Value.DTVAL:
                return lhs.get_dtVal() == rhs.get_dtVal()
            if rhs.getType() == Value.SVAL:
                ldt = lhs.get_dtVal()
                ldts = "%d-%02d-%02dT%02d:%02d:%02d.%06d" % (
                    ldt.year, ldt.month, ldt.day, ldt.hour, ldt.minute,
                    ldt.sec, ldt.microsec)
                rv = rhs.get_sVal()
                return ldts == rv if type(rv) == str else self.b(ldts) == rv
            return False
        if lhs.getType() == Value.LVAL:
            if not rhs.getType() == Value.LVAL:
                return False
            lvals = lhs.get_lVal().values
            rvals = rhs.get_lVal().values
            return self.compare_list(lvals, rvals)
        if lhs.getType() == Value.UVAL:
            if not rhs.getType() == Value.UVAL:
                return False
            lvals = lhs.get_uVal().values
            rvals = rhs.get_uVal().values
            return self._compare_list(lvals, rvals, self.compare_value)
        if lhs.getType() == Value.MVAL:
            if not rhs.getType() == Value.MVAL:
                return False
            lkvs = lhs.get_mVal().kvs
            rkvs = rhs.get_mVal().kvs
            return self.compare_map(lkvs, rkvs)
        if lhs.getType() == Value.VVAL:
            if not rhs.getType() == Value.VVAL:
                return False
            return self.compare_node(lhs.get_vVal(), rhs.get_vVal())
        if lhs.getType() == Value.EVAL:
            if not rhs.getType() == Value.EVAL:
                return False
            return self.compare_edge(lhs.get_eVal(), rhs.get_eVal())
        if lhs.getType() == Value.PVAL:
            if not rhs.getType() == Value.PVAL:
                return False
            return self.compare_path(lhs.get_pVal(), rhs.get_pVal())
        return False

    def compare_path(self, lhs: Path, rhs: Path):
        if len(lhs.steps) != len(rhs.steps):
            return False
        lsrc, rsrc = lhs.src, rhs.src
        for (l, r) in zip(lhs.steps, rhs.steps):
            lreverse = l.type is not None and l.type < 0
            rreverse = r.type is not None and r.type < 0
            lsrc, ldst = (lsrc, l.dst) if not lreverse else (l.dst, lsrc)
            rsrc, rdst = (rsrc, r.dst) if not rreverse else (r.dst, rsrc)
            if not self.compare_node(lsrc, rsrc):
                return False
            if not self.compare_node(ldst, rdst):
                return False
            if self._strict:
                if l.ranking != r.ranking:
                    return False
                if r.name is None or l.name != self.bstr(r.name):
                    return False
                if r.props is None or not self.compare_map(l.props, r.props):
                    return False
            else:
                if r.ranking is not None and l.ranking != r.ranking:
                    return False
                if r.name is not None and l.name != self.bstr(r.name):
                    return False
                if not (r.props is None or self.compare_map(l.props, r.props)):
                    return False
            lsrc, rsrc = ldst, rdst
        return True

    def eid(self, e: Edge, etype: int):
        src, dst = e.src, e.dst
        if e.type is None:
            if etype < 0:
                src, dst = e.dst, e.src
        else:
            if etype != e.type:
                src, dst = e.dst, e.src
        if type(src) == str:
            src = self.bstr(src)
        if type(dst) == str:
            dst = self.bstr(dst)
        return src, dst

    def compare_edge(self, lhs: Edge, rhs: Edge):
        if self._strict:
            if not lhs.name == self.bstr(rhs.name):
                return False
            if not lhs.ranking == rhs.ranking:
                return False
            rsrc, rdst = self.eid(rhs, lhs.type)
            if lhs.src != rsrc or lhs.dst != rdst:
                return False
            if rhs.props is None or len(lhs.props) != len(rhs.props):
                return False
        else:
            if rhs.src is not None and rhs.dst is not None:
                rsrc, rdst = self.eid(rhs, lhs.type)
                if lhs.src != rsrc or lhs.dst != rdst:
                    return False
            if rhs.ranking is not None:
                if lhs.ranking != rhs.ranking:
                    return False
            if rhs.name is not None:
                if lhs.name != self.bstr(rhs.name):
                    return False
            if rhs.props is None:
                return True
        return self.compare_map(lhs.props, rhs.props)

    def bstr(self, vid) -> bytes:
        return self.b(vid) if type(vid) == str else vid

    def compare_node(self, lhs: Vertex, rhs: Vertex):
        rtags = []
        if self._strict:
            assert rhs.vid is not None
            if not lhs.vid == self.bstr(rhs.vid):
                return False
            if rhs.tags is None or len(lhs.tags) != len(rhs.tags):
                return False
            rtags = rhs.tags
        else:
            if rhs.vid is not None:
                if not lhs.vid == self.bstr(rhs.vid):
                    return False
            if rhs.tags is not None and len(lhs.tags) < len(rhs.tags):
                return False
            rtags = [] if rhs.tags is None else rhs.tags
        for tag in rtags:
            ltag = [[lt.name, lt.props] for lt in lhs.tags
                    if self.bstr(tag.name) == lt.name]
            if len(ltag) != 1:
                return False
            if self._strict:
                if tag.props is None:
                    return False
            else:
                if tag.props is None:
                    continue
            lprops = ltag[0][1]
            if not self.compare_map(lprops, tag.props):
                return False
        return True

    def compare_map(self, lhs: Dict[bytes, Value], rhs: KV):
        if len(lhs) != len(rhs):
            return False
        for lkey, lvalue in lhs.items():
            if lkey not in rhs:
                return False
            rvalue = rhs[lkey]
            if not self.compare_value(lvalue, rvalue):
                return False
        return True

    def compare_list(self, lhs: List[Value], rhs: List[Value]):
        return len(lhs) == len(rhs) and \
            self._compare_list(lhs, rhs, self.compare_value)

    def compare_row(self, lhs: Row, rhs: Row):
        if not len(lhs.values) == len(rhs.values):
            return False
        return all(
            self.compare_value(l, r) for (l, r) in zip(lhs.values, rhs.values))

    def _compare_list(self, lhs, rhs, cmp_fn, included=False):
        visited = []
        for rr in rhs:
            found = False
            for i, lr in enumerate(lhs):
                if i not in visited and cmp_fn(lr, rr):
                    visited.append(i)
                    found = True
                    break
            if not found:
                return False
        size = len(lhs)
        if included:
            return len(visited) <= size
        return len(visited) == size
