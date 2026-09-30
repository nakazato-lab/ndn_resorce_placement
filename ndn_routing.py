"""Register producer routes as client routes for NFD's NLSR readvertisement."""
import asyncio
import logging

from ndn.app import NDNApp
from ndn.app_support.nfd_mgmt import make_command, parse_response
from ndn.encoding import Name
from ndn.types import InterestNack, InterestTimeout, InterestCanceled, ValidationFailure

LOG = logging.getLogger(__name__)
CLIENT_ORIGIN = 65


class RoutedNDNApp(NDNApp):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._route_lock = asyncio.Lock()

    async def _route_command(self, verb, name):
        # Serialize signed commands and avoid identical millisecond timestamps.
        async with self._route_lock:
            await asyncio.sleep(0.002)
            try:
                _, _, content = await self.express_interest(
                    make_command('rib', verb, self.face, name=name, origin=CLIENT_ORIGIN),
                    lifetime=4000)
                response = parse_response(content)
                if response['status_code'] != 200:
                    LOG.error('%s %s failed: %s', verb, Name.to_str(name), response)
                    return False
                LOG.info('%s %s: origin=client (65)', verb, Name.to_str(name))
                return True
            except (InterestNack, InterestTimeout, InterestCanceled, ValidationFailure) as exc:
                LOG.error('%s %s failed: %s', verb, Name.to_str(name), exc)
                return False

    async def register(self, name, func, validator=None, need_raw_packet=False, need_sig_ptrs=False):
        name = Name.normalize(name)
        if func is not None:
            self.set_interest_filter(name, func, validator, need_raw_packet, need_sig_ptrs)
        success = await self._route_command('register', name)
        if not success and func is not None:
            self.unset_interest_filter(name)
        return success

    async def unregister(self, name):
        name = Name.normalize(name)
        if not await self._route_command('unregister', name):
            return False
        self.unset_interest_filter(name)
        return True
