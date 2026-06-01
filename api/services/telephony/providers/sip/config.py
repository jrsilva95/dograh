from typing import List, Literal, Optional

from pydantic import BaseModel, Field

class SIPConfigurationRequest(BaseModel):
    provider: Literal["sip"] = "sip"
    sip_server: str = Field(..., description="SIP server address")
    sip_port: int = Field(5060, description="SIP server port")
    username: str = Field(..., description="SIP username")
    password: str = Field(..., description="SIP password")
    my_ip: str = Field("192.168.3.34", description="Local IP address")
    my_sip_port: int = Field(5060, description="Local SIP port")
    from_numbers: List[str] = Field(default_factory=list, description="List of phone numbers to use")
    country: Optional[str] = Field(
        None,
        description="ISO-2 country code (e.g. 'BR', 'US') used to normalize "
        "inbound numbers to E.164. Operators usually send bare local digits "
        "in the To header; this hint lets us prepend the country dial code "
        "before matching against stored phone_numbers.",
    )

class SIPConfigurationResponse(BaseModel):
    provider: Literal["sip"] = "sip"
    sip_server: str
    sip_port: int
    username: str
    my_ip: str
    my_sip_port: int
    from_numbers: List[str]
    country: Optional[str] = None
