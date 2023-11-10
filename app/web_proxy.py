import asyncio
import time
from asyncio import Future, Task
from typing import Tuple, Optional, Dict, Union

from bson import ObjectId
from flask import request, make_response
from pyppeteer import connect
from pyppeteer.browser import Browser
from pyppeteer.launcher import get_ws_endpoint
from pyppeteer.network_manager import Request, Response
from pyppeteer.page import Page

from app import app, logger
from error_handler import error


class BrowserSlot(object):
    """
    Slot which can be occupied by a browser object
    """
    browser: Optional[Browser]
    page: Optional[Page]
    interceptor: 'Interceptor'
    _release_task: Optional[Task]

    class RELEASE_REASON(object):
        REQUEST = 0
        TIMEOUT = 1

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.free = True
        self.browser = None
        self.page = None
        self.interceptor = Interceptor()
        self._release_task = None

    async def use(self):
        # discovery url
        endpoint = get_ws_endpoint(f'http://{self.host}:{self.port}')

        # initiate browser
        self.browser = await connect(browserWSEndpoint=endpoint)
        self.page = await self.browser.newPage()
        self.interceptor.attach(self.page)
        self.free = False

    async def release(self):
        await self._release(self.RELEASE_REASON.REQUEST)

    def release_after(self, timeout):
        # if old task, cancel it
        self._cancel_release_after()

        async def task():
            await asyncio.sleep(timeout)
            await self._release(self.RELEASE_REASON.TIMEOUT)

        self._release_task = asyncio.create_task(task())

    async def _release(self, reason: int):
        try:
            # close browser
            await self.page.close()
            self.interceptor.clear()
            if reason != self.RELEASE_REASON.TIMEOUT:
                self._cancel_release_after()
            # ### Browser.close() will actually terminate the process;
            # we don't want it so skip for now.
            # await self.browser.close()

        finally:
            self.free = True
            self.browser = None
            self.page = None

    def _cancel_release_after(self):
        if self._release_task:
            self._release_task.cancel()
            self._release_task = None


class BrowserSession(object):
    """
    Session info
    """
    session_id: str
    _slot: BrowserSlot
    time_created: float
    time_last_used: float
    live_timeout: int
    inactivity_timeout: int

    def __init__(self, slot: BrowserSlot, live_timeout: int, inactivity_timeout: int):
        self.session_id = str(ObjectId())
        self._slot = slot
        self.time_created = time.time()
        self.time_last_used = time.time()
        self.live_timeout = live_timeout
        self.inactivity_timeout = inactivity_timeout

    @property
    def slot(self) -> BrowserSlot:
        self.time_last_used = time.time()
        return self._slot


class BrowserPool(object):
    """
    Pool of browser slot with limited capacity.
    We can request a browser and than use it by number.
    """
    CAPACITY = 1
    TIMEOUT = 10
    MAX_TIMEOUT = 60
    LIVE_TIMEOUT = 600
    session_map: dict[str, BrowserSession]

    def __init__(self):
        self.pool = [BrowserSlot(host='127.0.0.1', port=9222)]
        self.session_map = {}

    async def use_slot(self, timeout=TIMEOUT) -> Tuple[BrowserSlot, str]:
        # cleanup when trying to use new one, maybe better schedule it?
        await self._cleanup()

        timeout = self._timeout(timeout)
        for i in range(self.CAPACITY):
            slot = self.pool[i]
            if slot.free:
                await slot.use()

                session = BrowserSession(slot, live_timeout=self.LIVE_TIMEOUT,
                                         inactivity_timeout=timeout)
                self.session_map[session.session_id] = session

                return slot, session.session_id
        raise Exception('No browser slots available')

    def __getitem__(self, session_id: str) -> BrowserSlot:
        if session_id in self.session_map:
            session = self.session_map[session_id]
            return session.slot
        raise IndexError(session_id)

    async def end_session(self, session_id: str):
        if session_id in self.session_map:
            session = self.session_map[session_id]
            slot = session.slot
            await slot.release()
            del self.session_map[session_id]

    async def _cleanup(self):
        now = time.time()
        keys = list(self.session_map.keys())
        for session_id in keys:
            session = self.session_map[session_id]
            if (now - session.time_last_used > session.inactivity_timeout) \
                    or (now - session.time_created > session.live_timeout):
                await self.end_session(session_id)

    def _timeout(self, timeout: int):
        if not (0 < timeout <= self.MAX_TIMEOUT):
            raise Exception(f'Timeout must be in (0, {self.MAX_TIMEOUT}]')
        return timeout


class MissingRequestError(Exception):
    pass


class Interceptor(object):
    req_map: dict[str, Request]
    res_map: dict[str, Response]
    future_res_map: dict[str, Future]

    def __init__(self):
        self.req_map = {}
        self.res_map = {}
        self.future_res_map = {}

    def attach(self, page: Page):
        page.on('request', self._register_request)
        page.on('response', self._register_response)

    def clear(self):
        self.req_map.clear()
        self.res_map.clear()
        self.future_res_map.clear()

    async def get_response_headers(self, url: str, timeout=10) -> Dict[str, str]:
        r = await self._get_response(url, timeout=timeout)
        return r.headers

    async def get_response_bytes(self, url: str, timeout=10) -> bytes:
        r = await self._get_response(url, timeout=timeout)
        bytes = await r.buffer()
        return bytes

    def _register_request(self, r: Request):
        logger.debug("Request will be sent: %s", r.url)
        self.req_map[r.url] = r
        self.future_res_map[r.url] = Future()

    def _register_response(self, r: Response):
        logger.debug("Response received: %s", r.url)
        self.res_map[r.url] = r
        if r.url in self.future_res_map:
            self.future_res_map[r.url].set_result(r)

    async def _get_response(self, url: str, timeout=None):
        # 1 response already available, return it
        if url in self.res_map:
            return self.res_map[url]
        # 2 request is sent but response is have not been received yet, wait
        if url in self.future_res_map:
            response = await asyncio.wait_for(self.future_res_map[url], timeout)
            return response
        # 3 request has not been sent, in this case we must send it ourselves???
        # if this is some resource that has not been sent by the proxy browser
        # but is sent by client browser; but it is out of scope of this method
        raise MissingRequestError(url)


pool = BrowserPool()


@app.route('/proxy', methods=['GET'])
async def proxy():
    """
    Simple proxy, non-session etc.......
    We load target url, pass rendered HTML to the response.
    @return: HTML response
    """
    url = request.args.get('url')
    timeout = int(request.args.get('timeout', 10))
    if not url:
        raise Exception('Missing argument: url')
    slot, session_id = await pool.use_slot(timeout)
    try:
        response = await _open_page(slot, session_id, url, timeout)
        return response
    except Exception as e:
        return error(e)
    finally:
        # nothing, slot will release itself after timeout
        pass


@app.route('/proxy/session', methods=['GET'])
async def start_session():
    """
    Start proxy session
    @return: session ID
    """
    timeout = int(request.args.get('timeout', 10))
    _, session_id = await pool.use_slot(timeout)
    return {
        'id': session_id,
        'success': True,
    }


@app.route('/proxy/<session_id>', methods=['DELETE'])
async def delete_session(session_id):
    """
    Delete proxy session
    @param session_id: session ID
    @return: nothing
    """
    await pool.end_session(session_id)
    return {
        'success': True,
    }


@app.route('/proxy/<session_id>/goto/<path:url>', methods=['GET'])
async def goto(session_id, url):
    """
    Goto URL
    @param session_id: session ID
    @param url: URL
    @return: HTML response
    """
    timeout = int(request.args.get('timeout', 10))
    slot = pool[session_id]
    response = await _open_page(slot, session_id, url, timeout, sessionless=False)
    return response


@app.route('/proxy/<session_id>/ref/<path:url>', methods=['GET'])
async def ref(session_id, url):
    """
    Get resource which page refers to.

    Currently supports only GET resources.

    @param session_id: Page's session ID
    @param url: Resource URL
    @return: Resource as is.
    """
    interceptor = pool[session_id].interceptor
    body = await interceptor.get_response_bytes(url)
    headers = await interceptor.get_response_headers(url)
    return _make_response(body, headers)


async def _open_page(slot, session_id, url, timeout, sessionless=True):
    page = slot.page
    await page.goto(url, timeout=1000 * timeout)
    await _patch(page, session_id, sessionless)
    html_content = await page.content()
    headers = await slot.interceptor.get_response_headers(url, timeout=timeout)
    response = _make_response(html_content, headers)
    return response


async def _patch(page: Page, session_id: str, sessionless: bool):
    """
    Patch a page to make links pointed to our proxy.
    This consists of

    1. replace href and src links

    2. monkey patch `fetch` to redirect dynamic requests

    3. ... probably something else?

    @param page: Page to patch
    @return: None
    """
    new_url_prefix = f'/proxy/{session_id}/ref/'
    if sessionless:
        new_a_url_prefix = f'/proxy?url='
    else:
        new_a_url_prefix = f'/proxy/{session_id}/goto/'

    # this must include links and stylesheets
    elements = await page.querySelectorAll(':not(a)[href]')
    for element in elements:
        url = await element.getProperty('href')
        await page.evaluate('''
        (element, prefix, url) => {
            element.href = prefix + encodeURIComponent(url);
        }
        ''', element, new_url_prefix, url)

    elements = await page.querySelectorAll('a[href]')
    for element in elements:
        url = await element.getProperty('href')
        await page.evaluate('''
        (element, prefix, url) => {
            element.href = prefix + encodeURIComponent(url);
        }
        ''', element, new_a_url_prefix, url)

    # this must include scripts and images
    elements = await page.querySelectorAll('*[src]')
    for element in elements:
        url = await element.getProperty('src')
        await page.evaluate('''
        (element, prefix, url) => {
            element.src = prefix + encodeURIComponent(url);
        }
        ''', element, new_url_prefix, url)


def _make_response(content: Union[str, bytes], headers: Dict[str, str] = {}):
    PROXY_HEADERS = ['content-type']
    response = make_response(content)
    for header in PROXY_HEADERS:
        if header in headers:
            # pyppeteer converts header to lower
            response.headers[header] = headers[header.lower()]
    return response
