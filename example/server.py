#!/usr/bin/env python
# -*- coding: utf-8 -*-

############################################################
#
# Copyright (C) 2020 SenseDeal AI, Inc. All Rights Reserved
#
# Description:
#     pass
#
# Author: Li Xiuming
# Last Modified: 2020-11-24
############################################################

import sys
import grpc
from concurrent import futures
sys.path.append("./rpc")
from rpc import pdf_txt_pb2, pdf_txt_pb2_grpc
import os
import time
import json
from func_timeout import func_timeout,FunctionTimedOut
from remote_svc.pdf_txt import PdfTxtSvc
from remote_svc.pdf_txt.ttypes import Status,AlgReq,AlgRsp
import socket
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from lib.extractor import extract
from lib import pdfplumber as pp
from sense_core import log_info
from sense_core import config, log_info, log_exception, log_init_config
log_init_config('pdf_txt', config('log_path'))

_version = "v2.0.2"
_rpc_type = os.environ.get("RPC_TYPE", "grpc")
print("_rpc_type", _rpc_type)

_timeout = os.environ.get("TIMEOUT")
if _timeout is not None:
    try:
        _timeout = float(_timeout)
    except:
        _timeout = None
        log_info("TIMEOUT: {} can not be converted into FLOAT type.".format(_timeout))

_workers = os.environ.get("WORKERS")
if _workers is not None:
    try:
        _workers = int(_workers)
    except:
        _workers = 1
        log_info("WORKERS: {} can not be converted into INT type.".format(_workers))


def _build_td_head(td):
    rowspan = int(td[2]) - int(td[0])
    colspan = int(td[3]) - int(td[1])
    if colspan != 1 and rowspan != 1:
        return '<td colspan=' + str(colspan) + ' rowspan='+str(rowspan)+'>'
    if colspan != 1:
        return '<td colspan='+str(colspan)+'>'
    if rowspan != 1:
        return '<td rowspan='+str(rowspan)+'>'
    return '<td>'


def format_table_html(text):
    text = text.strip()
    if not text.startswith('<table>') or not text.endswith('</table>'):
        return ''
    lines = text.split('\n')
    tr_list = []
    last_tr_pos = ''
    td_list = []
    for line in lines:
        index = line.find('|')
        if index <= 0:
            continue
        point = line[0:index].split(',')
        if len(point) != 4:
            continue
        if index == len(line) - 1:
            point.append('')
        else:
            point.append(line[index+1:len(line)])
        if point[0] != last_tr_pos:
            if len(td_list) > 0:
                tr_list.append(td_list)
                td_list = []
        td_list.append(point)
        last_tr_pos = point[0]
    if len(td_list) > 0:
        tr_list.append(td_list)
    result = '<table>\n'
    for tr in tr_list:
        result += '<tr>\n'
        for td in tr:
            result += _build_td_head(td)
            result += td[4]
            result += '</td>'
        result += '\n</tr>\n'
    result += '</table>'
    return result


def deal_content_to_html(content):
    if content.find('<table>') == -1:
        content_list = content.split('\n')
        for index, co in enumerate(content_list):
            co = '<p>\n' + co + '</p>\n'
            content_list[index] = co
        return '<div>\n' + ''.join(content_list) + '\n</div>'
    content = content.replace('</table>', '<replace><table>')
    content_list = content.split('<table>')
    for index, table in enumerate(content_list):
        if table.find('<replace>') != -1:
            table = table.replace('<replace>', '')
            table = '<table>' + table + '</table>'
            content_list[index] = format_table_html(table)
        else:
            content_par_list = table.split('\n')
            for _index, co in enumerate(content_par_list):
                co = '<p>\n' + co + '\n</p>\n'
                content_par_list[_index] = co
            content_par = ''.join(content_par_list)
            content_list[index] = content_par
    content = ''.join(content_list)
    return '<div>\n' + content + '\n</div>'


class Pdftxt(pdf_txt_pb2_grpc.PdftxtServicer):
    def pdf_txt(self, request, context):
        _dict = {
            0: 'success',
            -1: 'file not exits',
            -2: 'pdf open files error',
            -3: 'pdf to txt error',
            -4: 'pdf to txt timeout',
            -9: 'other error happens'}

        _code = 0
        _msg = _dict[_code]
        _txt = ''
        pdf = None
        start = time.time()

        try:
            pdf = pp.open(request.filename)
        except Exception as e:
            _code = -2
            _msg = '{0}:{1}'.format(_dict[_code], e)
            end = time.time()
            log_info('request deal cost:{}, status:{}, msg:{}'.format((end - start), _code, _msg))
            return pdf_txt_pb2.Reply(publish_txt=_txt, status={"code": _code, "msg": _msg, "version":_version})

        try:
            if _timeout is not None:
                _txt = func_timeout(_timeout, extract, (pdf,))
            else:
                _txt = extract(pdf)
        except FunctionTimedOut:
            _code = -4
            _msg = '{0}:{1}'.format(_dict[_code], FunctionTimedOut)
            pdf.close()
            end = time.time()
            log_info('request deal cost:{}, status:{}, msg:{}'.format((end - start), _code, _msg))
            return pdf_txt_pb2.Reply(publish_txt=_txt, status={"code": _code, "msg": _msg, "version":_version})

        except Exception as e:
            _code = -3
            _msg = '{0}:{1}'.format(_dict[_code], e)
            pdf.close()
            end = time.time()
            log_info('request deal cost:{}, status:{}, msg:{}'.format((end - start), _code, _msg))
            return pdf_txt_pb2.Reply(publish_txt=_txt, status={"code": _code, "msg": _msg, "version":_version})

        pdf.close()
        end = time.time()
        log_info('request deal cost:{}, status:{}, msg:{}'.format((end - start), _code, _msg))
        return pdf_txt_pb2.Reply(publish_txt=deal_content_to_html(_txt), status={"code": _code, "msg": _msg, "version":_version})


def grpc_serve(host, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=_workers))
    pdf_txt_pb2_grpc.add_PdftxtServicer_to_server(Pdftxt(), server)
    server.add_insecure_port(host + ':' + port)
    server.start()
    print("Starting python server...")

    _ONE_DAY_IN_SECONDS = 60 * 60 * 24
    while True:
         time.sleep(_ONE_DAY_IN_SECONDS)



class PdfTxtExecuter:
    def handler(self, alg_req):

        _dict = {
            0: 'success',
            -1: 'file not exits',
            -2: 'pdf open files error',
            -3: 'pdf to txt error',
            -4: 'pdf to txt timeout',
            -9: 'other error happens'}

        _code = 0
        _msg = _dict[_code]
        _txt = ''
        pdf = None
        start = time.time()

        try:
            pdf = pp.open(alg_req.filename)
        except Exception as e:
            _code = -2
            _msg = '{0}:{1}'.format(_dict[_code], e)
            end = time.time()
            log_info('request deal cost:{}, status:{}, msg:{}'.format((end - start), _code, _msg))
            stat = Status(code=_code, msg=_msg, version=_version)
            return AlgRsp(result=_txt, status=stat)


        try:
            if _timeout is not None:
                 _txt = func_timeout(_timeout, extract, (pdf,))
            else:
                _txt = extract(pdf)
        except FunctionTimedOut:
            _code = -4
            _msg = '{0}:{1}'.format(_dict[_code], FunctionTimedOut)
            pdf.close()
            end = time.time()
            log_info('request deal cost:{}, status:{}, msg:{}'.format((end - start), _code, _msg))
            stat = Status(code=_code, msg=_msg, version=_version)
            return AlgRsp(result=_txt, status=stat)

        except Exception as e:
            _code = -3
            _msg = '{0}:{1}'.format(_dict[_code], e)
            pdf.close()
            end = time.time()
            log_info('request deal cost:{}, status:{}, msg:{}'.format((end - start), _code, _msg))
            stat = Status(code=_code, msg=_msg, version=_version)
            return AlgRsp(result=_txt, status=stat)

        pdf.close()
        end = time.time()
        log_info('request deal cost:{}, status:{}, msg:{}'.format((end - start), _code, _msg))
        stat = Status(code=_code, msg=_msg, version=_version)
        res = AlgRsp(result=_txt, status=stat)
        return res


def thrift_serve(host, port):
    processor = PdfTxtSvc.Processor(PdfTxtExecuter())
    transport = TSocket.TServerSocket(host, port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    print("Starting python server...")
    server.serve()


def serve():
    host = "0"
    port = "5000"
    print("host", host)
    print("port", port)

    if _rpc_type == "grpc":
        grpc_serve(host, port)
    elif _rpc_type == "thrift":
        thrift_serve(host, port)


if __name__=="__main__":
    serve()