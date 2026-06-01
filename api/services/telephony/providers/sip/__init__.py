"""SIP telephony provider package using pyVoIP."""

from typing import Any, Dict

from api.services.telephony.registry import (
    ProviderSpec,
    ProviderUIField,
    ProviderUIMetadata,
    register,
)

from .config import SIPConfigurationRequest, SIPConfigurationResponse
from .provider import SIPProvider
from .sip_manager import startup_phones
from .transport import create_transport


def _config_loader(value: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "provider": "sip",
        "sip_server": value.get("sip_server"),
        "sip_port": value.get("sip_port", 5060),
        "username": value.get("username"),
        "password": value.get("password"),
        "my_ip": value.get("my_ip", "192.168.3.34"),
        "my_sip_port": value.get("my_sip_port", 5060),
        "from_numbers": value.get("from_numbers", []),
        "country": value.get("country"),
    }


_UI_METADATA = ProviderUIMetadata(
    display_name="SIP (pyVoIP)",
    fields=[
        ProviderUIField(
            name="sip_server",
            label="SIP Server",
            type="text",
            description="SIP server address (e.g., sip.example.com)",
        ),
        ProviderUIField(
            name="sip_port",
            label="SIP Port",
            type="number",
            description="SIP server port (default: 5060)",
        ),
        ProviderUIField(
            name="username",
            label="Username",
            type="text",
        ),
        ProviderUIField(
            name="password",
            label="Password",
            type="password",
            sensitive=True,
        ),
        ProviderUIField(
            name="my_ip",
            label="Local IP",
            type="text",
            description="Local IP address for SIP registration",
        ),
        ProviderUIField(
            name="my_sip_port",
            label="Local SIP Port",
            type="number",
            description="Local SIP port (default: 5060)",
        ),
        ProviderUIField(
            name="from_numbers",
            label="From Numbers",
            type="string-array",
            description="List of phone numbers to use for outbound calls",
        ),
        ProviderUIField(
            name="country",
            label="Country (ISO-2)",
            type="text",
            description="ISO-2 country code (e.g. 'BR', 'US') for inbound "
            "number normalization. Required when operator sends bare local "
            "digits without country prefix.",
        ),
    ],
)


SPEC = ProviderSpec(
    name="sip",
    provider_cls=SIPProvider,
    config_loader=_config_loader,
    transport_factory=create_transport,
    transport_sample_rate=8000,
    config_request_cls=SIPConfigurationRequest,
    ui_metadata=_UI_METADATA,
    config_response_cls=SIPConfigurationResponse,
)


register(SPEC)


__all__ = [
    "SPEC",
    "SIPConfigurationRequest",
    "SIPConfigurationResponse",
    "SIPProvider",
    "create_transport",
    "startup_phones",
]
