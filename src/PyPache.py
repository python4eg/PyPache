import os
import sys
import stat

import ConfigParser
import cStringIO
import logging
import mimetypes
import optparse
import StringUtils
import socket
import ThreadPool
import time

APP_PATH = '.'
CONFIG_PATH = os.parh.join(APP_PATH, 'etc', 'server_config.conf')
PACKET_SIZE = 4096
SERVER_NAME = 'PyPache'
SERVER_VERSION = '0.0.1'
DATE_FORMAT = '%a, %d %b %Y %H:%M:%S'
DATE_FORMAT_LITE = '%d.%m.%Y %H:%M'
ER_NOT_FOUND = 2
ER_FORBIDDEN = 13

RESPONSES = {
            100: ('Continue', 'Request received, please continue'),
            101: ('Switching Protocols','Switching to new protocol; obey Upgrade header'),
            200: ('OK', 'Request fulfilled, document follows'),
            201: ('Created', 'Document created, URL follows'),
            202: ('Accepted','Request accepted, processing continues off-line'),
            203: ('Non-Authoritative Information', 'Request fulfilled from cache'),
            204: ('No response', 'Request fulfilled, nothing follows'),
            205: ('Reset Content', 'Clear input form for further input.'),
            206: ('Partial Content', 'Partial content follows.'),
            300: ('Multiple Choices', 'Object has several resources -- see URI list'),
            301: ('Moved Permanently', 'Object moved permanently -- see URI list'),
            302: ('Found', 'Object moved temporarily -- see URI list'),
            303: ('See Other', 'Object moved -- see Method and URL list'),
            304: ('Not modified', 'Document has not changed since given time'),
            305: ('Use Proxy', 'Use proxy specified in Location to access this resource.'),
            307: ('Temporary Redirect', 'Object moved temporarily -- see URI list'),
            400: ('Bad request', 'Bad request syntax or unsupported method'),
            401: ('Unauthorized', 'No permission -- see authorization schemes'),
            402: ('Payment required', 'No payment -- see charging schemes'),
            403: ('Forbidden', 'Request forbidden'),
            404: ('Not Found', 'No such file or directory'),
            405: ('Method Not Allowed', 'Specified method is invalid for this server.'),
            406: ('Not Acceptable', 'URI not available in preferred format.'),
            407: ('Proxy Authentication Required', 'You must authenticate with '
                  'this proxy before proceeding.'),
            408: ('Request Time-out', 'Request timed out; try again later.'),
            409: ('Conflict', 'Request conflict.'),
            410: ('Gone', 'URI no longer exists and has been permanently removed.'),
            411: ('Length Required', 'Client must specify Content-Length.'),
            412: ('Precondition Failed', 'Precondition in headers is false.'),
            413: ('Request Entity Too Large', 'Entity is too large.'),
            414: ('Request-URI Too Long', 'URI is too long.'),
            415: ('Unsupported Media Type', 'Entity body in unsupported format.'),
            416: ('Requested Range Not Satisfiable', 'Cannot satisfy request range.'),
            417: ('Expectation Failed', 'Expect condition could not be satisfied.'),
            500: ('Internal error', 'Server got itself in trouble'),
            501: ('Not Implemented', 'Server does not support this operation'),
            502: ('Bad Gateway', 'Invalid responses from another server/proxy.'),
            503: ('Service temporarily overloaded',
                  'The server cannot process the request due to a high load'),
            504: ('Gateway timeout', 'The gateway server did not receive a timely response'),
            505: ('HTTP Version not supported', 'Cannot fulfill request.'),
            }


def log_init(level):
    format = '%(asctime)s - %(name)-12s %(levelname)-10s %(message)s'
    datefmt = '%a, %d %b %Y %H:%M:%S'
    logging.basicConfig(level=level, format=format,
                        datefmt=datefmt)


def get_connection_kwargs(path):
    config_section_name = 'connection'
    config = ConfigParser.ConfigParser()
    config.read(path)
    recv_host = config.get(config_section_name, 'recv_host')
    port = config.getint(config_section_name, 'port')
    max_conn = config.getint(config_section_name, 'max_connection')
    return {'recv_host': recv_host,
            'port': port,
            'max_conn': max_conn
            }

def get_location_args():
        config_section_name = 'location'
        config = ConfigParser.ConfigParser()
        config.read(CONFIG_PATH)
        root_path = config.get(config_section_name, 'root_path')
        start_page = config.get(config_section_name, 'start_page')
        dir_list = config.getboolean(config_section_name, 'directory_listing')
        return (root_path, start_page, dir_list,)

def get_options():
    parse = optparse.OptionParser(version=SERVER_VERSION)
    parse.add_option('-c', '--config_file', action='store', dest='conf',
                    help='Path to config file', type='string', default=CONFIG_PATH)
    parse.add_option('-v', '--verbose', action='store', dest='verb',
                    help='Level', type='choice',
                    choices=('DEBUG','WARNING','CRITICAL','ERROR'), default = 'DEBUG')
    (option, args) = parse.parse_args()
    return option


class PyPacheServer:
    def __init__(self, recv_host, port, max_conn):
        self.__host = recv_host
        self.__port = port
        self.__max_conn = max_conn
        self.__logger = logging.getLogger('Server')
        self.__pool = ThreadPool.ThreadPool(Client, 10, 10, 1)

    def __init_sock(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind((self.__host, self.__port))
            sock.listen(self.__max_conn)
            return sock
        except Exception, e:
            self.__logger.error('Init socket Error: %s', e)

    def start(self):
        try:
            self.__sock = self.__init_sock()
            while True:
                get_client = self.__sock.accept()
                self.__logger.debug('Connection from %s established', get_client[1])
                self.__logger.debug('Wait to data from %s', get_client[1])
                self.__pool.add_task(get_client)
                self.__logger.debug('Complite')
        except socket.error, e:
            self.__logger.error('Socket Error: %s', e)

    def stop(self):
        self.__sock.close()
        self.__logger.debug('Socket close. Server is stopped')

class Client:
    encodings_map = mimetypes.types_map.copy()
    encodings_map.update({'.ico':'image/x-icon','.pys':'text/html', '.html':'text/html'})
    __root_path,__start_page, __dir_list = get_location_args()

    def __init__(self, client):
        self.__conn, self.__addr = client
        self.__logger = logging.getLogger('Client')
        self.start()

    def send_response(self, http, code, message=None):
        if message is None:
            if code in RESPONSES:
                message = RESPONSES[code][0]
            else:
                message = ''

        ret_data = '%s %d %s\r\n' % (http, code, message)
        self.__conn.send(ret_data)

    def send_header(self, keyword, value):
        ret_data = '%s: %s\r\n' % (keyword, value)
        self.__conn.send(ret_data)

    def send_end_header(self):
        self.__conn.send('\r\n')

    def date_time_string(self):
        s = time.strftime(DATE_FORMAT + ' GMT', time.gmtime())
        return s

    def gen_page_error(self, response, message, path):
        f = cStringIO.StringIO()
        if response in RESPONSES:
            r_msg1 = RESPONSES[response][0]
            r_msg2 = RESPONSES[response][1]
        else:
            r_msg1 = ''
            r_msg2 = ''

        f.write('<html><title>%s %s</title>\n' % (response, r_msg1,))
        f.write('<body>\n')
        f.write('<h1>ERROR %s: %s</h1>\n' % (response, r_msg2,))
        f.write('<h5>%s: <u>%s</u></h5>\n' % (message, path))
        f.write('<hr><address>%s</address>\n' % (SERVER_NAME,))
        f.write('</body></html>')
        f.seek(0)
        return f

    def gen_directory_list(self, host, path):
        f = cStringIO.StringIO()
        list = os.listdir(self.__root_path+path)
        back_path = os.pardir
        name_len = 32
        f.write('<html><head>\n')
        f.write('<title>Directory listing for %s</title>\n' % (path,))
        f.write('</head>\n')
        f.write('<body style = "font-family:verdana,sans-serif">\n')
        f.write('<h2>Directory listing for %s%s</h2>\n' % (host, path,))
        f.write('<pre><table><tr>\n')
        f.write('<td colspan="3"><a href="..">Parent Directory</a></td>\n')
        f.write('</tr><tr align="left">\n')
        f.write('<th width="320px">Name</th>\n')
        f.write('<th width="100px">Size</th>\n')
        f.write('<th width="200px">Last Modified</th>\n')
        f.write('</tr>\n')
        for name in list:
            rel_path = path + name
            abs_path = self.__root_path + path + name
            stats = os.stat(abs_path)
            if len(name) > name_len:
                name = name[:name_len] + '>'
            if os.path.isdir(abs_path):
                rel_path += '/'
                fsize = 'Directory'
            else:
                size = stats[stat.ST_SIZE]
                fsize = StringUtils.size_to_maxbyte(size)
            modif_time = time.strftime(DATE_FORMAT_LITE, time.localtime(stats[stat.ST_MTIME]))
            f.write('<tr>\n')
            f.write('<td><a href=%s>%s</div></a></td>\n' % (rel_path, name,))
            f.write('<td>%s</td>\n' % (fsize,))
            f.write('<td>%s</td>\n' % (modif_time,))
            f.write('</tr>\n')
        f.write('</table></pre>\n')
        f.write('<hr><address>%s</address>\n'% (SERVER_NAME,))
        f.write('</body></html>')
        f.seek(0)
        return f

    def send_headers(self, http, response, ctype, fsize):
        self.send_response(http, response)
        self.send_header('Content-type', ctype)
        self.send_header('Content-Length', fsize)
        self.send_header('Server', SERVER_NAME)
        self.send_header('Date', self.date_time_string())
        self.send_end_header()

    def send_error(self, http, errno, path):
        ctype = 'text/html'
        if errno == ER_NOT_FOUND:
            response = 404
        elif errno == ER_FORBIDDEN:
            response = 403
        message = RESPONSES[response][1]

        f = self.gen_page_error(response, message, path)
        fsize = len(f.getvalue())
        self.send_response(http, response)
        self.send_header('Content-type', ctype)
        self.send_header('Content-Length', fsize)
        self.send_header('Server', SERVER_NAME)
        self.send_header('Date', self.date_time_string())
        self.send_end_header()
        return f

    def sending(self, args):
        f = self.send_data(**args)
        data = f.readlines()
        sent_cnt = 0
        while sent_cnt < len(data):
            sent_cnt += self.__conn.send(''.join(data[sent_cnt:]))
        f.close()

    def send_data(self, path, http, args, host, agent):
        file_name = path
        print file_name
        if file_name.endswith('/'):
            if not os.path.exists(self.__root_path+file_name):
                return self.send_error(http, ER_NOT_FOUND, file_name)
            if not os.path.exists(self.__root_path+file_name+self.__start_page):
                if not self.__dir_list:
                    return self.send_error(http, ER_FORBIDDEN, file_name)
                ctype = 'text/html'
                response = 200
                f = self.gen_directory_list(host, file_name)
                fsize = len(f.getvalue())
                self.send_headers(http, response, ctype, fsize)
                return f
            else:
                file_name += self.__start_page

        type = StringUtils.type_split(file_name)
        if type in self.encodings_map:
            type.lower()
            ctype = self.encodings_map[type]
        else:
            ctype = 'text/plain'

        mode = 'rb'
        relative_path = file_name
        file_name = StringUtils.urlstoc(file_name)
        file_name = self.__root_path + file_name
        file_name = unicode(file_name, 'cp866')
        print file_name
        try:
            f = open(file_name, mode)
            response = 200
            fsize = os.stat(file_name)[stat.ST_SIZE]
            self.send_headers(http, response, ctype, fsize)
        except IOError, (errno, strerror):
            return self.send_error(http, errno, relative_path)
        return f

    def start(self):
        try:
            recv_data = self.__conn.recv(PACKET_SIZE)
            self.__logger.debug('Recv:\n%s', recv_data)
            url_args = StringUtils.get_args(recv_data)
            self.sending(url_args)
        finally:
            self.stop()

    def stop(self):
        self.__conn.close()

def main():
    opts = get_options()
    level = logging.getLevelName(opts.verb)
    log_init(level)
    conf_args = get_connection_kwargs(opts.conf)
    serv = PyPacheServer(**conf_args)
    serv.start()

if __name__ == '__main__':
    sys.exit(main())
