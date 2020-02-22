import enum
from functools import wraps
import logging
import time
from threading import Condition, Lock

import automationhat


logger = logging.getLogger(__name__)


def sleep_until(ts):
    """Sleeps until time.monotonic() is greater than the given timestamp.
    """
    time.sleep(max(0, ts - time.monotonic()))


class FlippedRelayWrapper(object):
    """Wraps an Automation HAT relay object, reversing its apparent state.
    (The wrapper's on() method turns the underlying relay off, etc.)
    """

    def __init__(relay):
        self._relay = relay

    def read(self, value):
        return not self._relay.read()

    def write(self, value):
        self._relay.write(not value)

    def is_on(self):
        return self._relay.is_off()

    def is_off(self):
        return self._relay.is_on()

    def on(self):
        self._relay.off()

    def off(self):
        self._relay.on()

    def toggle(self):
        self._relay.toggle()


class MethodCallLoggingWrapper(object):
    """Wraps an arbitrary object and logs method calls at DEBUG level.
    """

    def __init__(self, delegate, delegate_repr=None):
        self._delegate = delegate
        self._delegate_repr = delegate_repr or repr(delegate)

    def __getattr__(self, name):
        attr = getattr(self._delegate, name)
        if callable(attr) and logger.isEnabledFor(logging.DEBUG):
            @wraps(attr)
            def wrapper(*args, **kwargs):
                args_str = ', '.join(
                    [str(x) for x in args] + [f'{k}={v}' for k, v in kwargs.items()])
                logger.debug(f'{self._delegate_repr}.{name}({args_str})')
                return attr(*args, **kwargs)
            return wrapper
        return attr


class State(enum.Enum):
    OFF = 0x0
    STROBE_ONLY = 0x9
    HORN_STEADY = 0xb
    HORN_TEMPORAL = 0x8
    HORN_MARCH_TIME = 0xe


_STATE_BITS = 4

_GLOBAL_LOCK = Lock()

_RELAYS = [MethodCallLoggingWrapper(FlippedRelayWrapper(automationhat.relay.one), 'relay.one'),
           MethodCallLoggingWrapper(FlippedRelayWrapper(automationhat.relay.two), 'relay.two')]
_OUTPUT = MethodCallLoggingWrapper(automationhat.output.three, 'output.three')

_STROBE_SYNC_INTERVAL_SEC = 0.98
_STROBE_SYNC_PRE_EMBED_DURATION_SEC = 0.004
_STROBE_SYNC_POST_EMBED_DURATION_SEC = 0.002

_HORN_EMBED_INTERVAL_ITERS = 8
_HORN_EMBED_BIT_DURATION_SEC = 0.001


def disable_auto_lights():
    """Disables automatic light operation on relevant HAT relays/outputs.
    """
    for r in _RELAYS:
        r.auto_light(False)
    _OUTPUT.auto_light(False)


class Appliance(object):
    """Controls a SmartSync notification appliance via Automation HAT.
    """

    def __init__(self):
        self._state = State.OFF
        self._lock = Lock()
        self._state_updated = Condition(self._lock)

    @property
    def state(self):
        return self._state

    @state.setter
    def set_state(self, state):
        with self._lock:
            self._state = state
            self._state_updated.notify()

    def driver_loop(self, *args, **kwargs):
        """Enters a loop that drives the appliance.

        The calling thread will periodically send strobe synchronization pulses
        and horn control signals to the appliance, observing changes in the
        state attribute of this Appliance instance and responding appropriately.
        If an optional (non-negative) duration_secs argument is given, the loop
        loop exits after the duration has elapsed, returning the appliance to
        standby mode.

        Only one thread (across all Appliance instances) can drive the appliance
        at a time; overlapping calls will produce an exception.
        """
        if not _GLOBAL_LOCK.acquire(blocking=False):
            raise RuntimeError('Only one thread can drive the appliance at a time')
        try:
            self._driver_loop_impl(*args, **kwargs)
        finally:
            _GLOBAL_LOCK.release()

    def _driver_loop_impl(self, duration_secs=-1):
        if duration_secs < 0 and duration_secs != -1:
            raise ValueError('Duration must be non-negative')

        start_ts = time.monotonic()
        iters_in_state = 0
        last_state = None

        # Verify that our relays etc. are in the expected standby state
        if not all(r.is_off() for r in _RELAYS) and _OUTPUT.is_on():
            raise RuntimeError('Unexpected Automation HAT state entering driver loop')

        try:
            while True:
                # Break if we're over time
                iter_ts = time.monotonic()
                if duration_secs == -1:
                    timeout = None
                elif iter_ts - start_ts > duration_secs:
                    break
                else:
                    timeout = start_ts + duration_secs - iter_ts

                with self._lock:
                    if self._state == State.OFF:
                        # Standby and wait for a non-OFF state, or until we're over time
                        self._standby()
                        self._state_updated.wait_for(lambda: self._state != State.OFF, timeout)
                    else:
                        # Otherwise, send a strobe synchronization pulse
                        # Embed a horn control signal periodically or after a state change
                        if self._state != last_state:
                            iters_in_state = 0
                        self._pulse(embed_horn_signal=iters_in_state % _HORN_EMBED_INTERVAL_ITERS == 0)
                        last_state = self._state
                        iters_in_state += 1

                # Sleep until 1s after the last iteration started
                sleep_until(iter_ts + 1)

        finally:
            self._standby()
            # Sleep a few seconds, retaining the lock, to prevent back-to-back
            # calls sending a jumble of signals
            time.sleep(5)

    def _standby(self):
        for r in _RELAYS:
            r.off()
        _OUTPUT.on()

    def _pulse(self, embed_horn_signal):
        target_ts = time.monotonic()

        # If we're not embedding a horn signal, no need to change polarity/use
        # the relays; we can just send the strobe synchronization pulse using
        # the output
        if not embed_horn_signal:
            _OUTPUT.off()
            sleep_until(target_ts + _STROBE_SYNC_PRE_EMBED_DURATION_SEC +
                        _HORN_EMBED_BIT_DURATION_SEC * _STATE_BITS +
                        _STROBE_SYNC_POST_EMBED_DURATION_SEC)
            _OUTPUT.on()
            return

        # Disable output, set relays to reverse polarity
        _OUTPUT.off()
        for r in _RELAYS:
            r.off()
        target_ts += _STROBE_SYNC_PRE_EMBED_DURATION_SEC
        sleep_until(target_ts)

        # Send horn signal bits
        for i in reversed(range(_STATE_BITS)):
            _OUTPUT.write(self._state.value & 1 << i != 0)
            target_ts += _HORN_EMBED_BIT_DURATION_SEC
            sleep_until(target_ts)

        # Disable output if enabled, set relays to regular polarity
        _OUTPUT.off()
        for r in _RELAYS:
            r.on()
        target_ts += _STROBE_SYNC_POST_EMBED_DURATION_SEC
        sleep_until(target_ts)

        _OUTPUT.on()
