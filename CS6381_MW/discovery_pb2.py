# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: discovery.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='discovery.proto',
  package='',
  syntax='proto3',
  serialized_options=None,
  serialized_pb=_b('\n\x0f\x64iscovery.proto\"8\n\x0eRegistrantInfo\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04\x61\x64\x64r\x18\x02 \x01(\t\x12\x0c\n\x04port\x18\x03 \x01(\r\"T\n\x0bRegisterReq\x12\x13\n\x04role\x18\x01 \x01(\x0e\x32\x05.Role\x12\x1d\n\x04info\x18\x02 \x01(\x0b\x32\x0f.RegistrantInfo\x12\x11\n\ttopiclist\x18\x03 \x03(\t\"7\n\x0cRegisterResp\x12\x17\n\x06status\x18\x01 \x01(\x0e\x32\x07.Status\x12\x0e\n\x06reason\x18\x02 \x01(\t\"\x0c\n\nIsReadyReq\"\x1d\n\x0bIsReadyResp\x12\x0e\n\x06status\x18\x01 \x01(\x08\"(\n\x13LookupPubByTopicReq\x12\x11\n\ttopiclist\x18\x01 \x03(\t\";\n\x14LookupPubByTopicResp\x12#\n\npublishers\x18\x01 \x03(\x0b\x32\x0f.RegistrantInfo\"\x12\n\x10LookupAllPubsReq\"8\n\x11LookupAllPubsResp\x12#\n\npublishers\x18\x01 \x03(\x0b\x32\x0f.RegistrantInfo\"\xd6\x01\n\x0c\x44iscoveryReq\x12\x1b\n\x08msg_type\x18\x01 \x01(\x0e\x32\t.MsgTypes\x12$\n\x0cregister_req\x18\x02 \x01(\x0b\x32\x0c.RegisterReqH\x00\x12\"\n\x0bisready_req\x18\x03 \x01(\x0b\x32\x0b.IsReadyReqH\x00\x12*\n\nlookup_req\x18\x04 \x01(\x0b\x32\x14.LookupPubByTopicReqH\x00\x12(\n\x0b\x61llpubs_req\x18\x05 \x01(\x0b\x32\x11.LookupAllPubsReqH\x00\x42\t\n\x07\x43ontent\"\xdf\x01\n\rDiscoveryResp\x12\x1b\n\x08msg_type\x18\x01 \x01(\x0e\x32\t.MsgTypes\x12&\n\rregister_resp\x18\x02 \x01(\x0b\x32\r.RegisterRespH\x00\x12$\n\x0cisready_resp\x18\x03 \x01(\x0b\x32\x0c.IsReadyRespH\x00\x12,\n\x0blookup_resp\x18\x04 \x01(\x0b\x32\x15.LookupPubByTopicRespH\x00\x12*\n\x0c\x61llpubs_resp\x18\x05 \x01(\x0b\x32\x12.LookupAllPubsRespH\x00\x42\t\n\x07\x43ontent*P\n\x04Role\x12\x10\n\x0cROLE_UNKNOWN\x10\x00\x12\x12\n\x0eROLE_PUBLISHER\x10\x01\x12\x13\n\x0fROLE_SUBSCRIBER\x10\x02\x12\r\n\tROLE_BOTH\x10\x03*\\\n\x06Status\x12\x12\n\x0eSTATUS_UNKNOWN\x10\x00\x12\x12\n\x0eSTATUS_SUCCESS\x10\x01\x12\x12\n\x0eSTATUS_FAILURE\x10\x02\x12\x16\n\x12STATUS_CHECK_AGAIN\x10\x03*y\n\x08MsgTypes\x12\x10\n\x0cTYPE_UNKNOWN\x10\x00\x12\x11\n\rTYPE_REGISTER\x10\x01\x12\x10\n\x0cTYPE_ISREADY\x10\x02\x12\x1c\n\x18TYPE_LOOKUP_PUB_BY_TOPIC\x10\x03\x12\x18\n\x14TYPE_LOOKUP_ALL_PUBS\x10\x04\x62\x06proto3')
)

_ROLE = _descriptor.EnumDescriptor(
  name='Role',
  full_name='Role',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='ROLE_UNKNOWN', index=0, number=0,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='ROLE_PUBLISHER', index=1, number=1,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='ROLE_SUBSCRIBER', index=2, number=2,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='ROLE_BOTH', index=3, number=3,
      serialized_options=None,
      type=None),
  ],
  containing_type=None,
  serialized_options=None,
  serialized_start=889,
  serialized_end=969,
)
_sym_db.RegisterEnumDescriptor(_ROLE)

Role = enum_type_wrapper.EnumTypeWrapper(_ROLE)
_STATUS = _descriptor.EnumDescriptor(
  name='Status',
  full_name='Status',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='STATUS_UNKNOWN', index=0, number=0,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='STATUS_SUCCESS', index=1, number=1,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='STATUS_FAILURE', index=2, number=2,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='STATUS_CHECK_AGAIN', index=3, number=3,
      serialized_options=None,
      type=None),
  ],
  containing_type=None,
  serialized_options=None,
  serialized_start=971,
  serialized_end=1063,
)
_sym_db.RegisterEnumDescriptor(_STATUS)

Status = enum_type_wrapper.EnumTypeWrapper(_STATUS)
_MSGTYPES = _descriptor.EnumDescriptor(
  name='MsgTypes',
  full_name='MsgTypes',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='TYPE_UNKNOWN', index=0, number=0,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='TYPE_REGISTER', index=1, number=1,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='TYPE_ISREADY', index=2, number=2,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='TYPE_LOOKUP_PUB_BY_TOPIC', index=3, number=3,
      serialized_options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='TYPE_LOOKUP_ALL_PUBS', index=4, number=4,
      serialized_options=None,
      type=None),
  ],
  containing_type=None,
  serialized_options=None,
  serialized_start=1065,
  serialized_end=1186,
)
_sym_db.RegisterEnumDescriptor(_MSGTYPES)

MsgTypes = enum_type_wrapper.EnumTypeWrapper(_MSGTYPES)
ROLE_UNKNOWN = 0
ROLE_PUBLISHER = 1
ROLE_SUBSCRIBER = 2
ROLE_BOTH = 3
STATUS_UNKNOWN = 0
STATUS_SUCCESS = 1
STATUS_FAILURE = 2
STATUS_CHECK_AGAIN = 3
TYPE_UNKNOWN = 0
TYPE_REGISTER = 1
TYPE_ISREADY = 2
TYPE_LOOKUP_PUB_BY_TOPIC = 3
TYPE_LOOKUP_ALL_PUBS = 4



_REGISTRANTINFO = _descriptor.Descriptor(
  name='RegistrantInfo',
  full_name='RegistrantInfo',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='RegistrantInfo.id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='addr', full_name='RegistrantInfo.addr', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='port', full_name='RegistrantInfo.port', index=2,
      number=3, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=19,
  serialized_end=75,
)


_REGISTERREQ = _descriptor.Descriptor(
  name='RegisterReq',
  full_name='RegisterReq',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='role', full_name='RegisterReq.role', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='info', full_name='RegisterReq.info', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='topiclist', full_name='RegisterReq.topiclist', index=2,
      number=3, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=77,
  serialized_end=161,
)


_REGISTERRESP = _descriptor.Descriptor(
  name='RegisterResp',
  full_name='RegisterResp',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='status', full_name='RegisterResp.status', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='reason', full_name='RegisterResp.reason', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=163,
  serialized_end=218,
)


_ISREADYREQ = _descriptor.Descriptor(
  name='IsReadyReq',
  full_name='IsReadyReq',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=220,
  serialized_end=232,
)


_ISREADYRESP = _descriptor.Descriptor(
  name='IsReadyResp',
  full_name='IsReadyResp',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='status', full_name='IsReadyResp.status', index=0,
      number=1, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=234,
  serialized_end=263,
)


_LOOKUPPUBBYTOPICREQ = _descriptor.Descriptor(
  name='LookupPubByTopicReq',
  full_name='LookupPubByTopicReq',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='topiclist', full_name='LookupPubByTopicReq.topiclist', index=0,
      number=1, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=265,
  serialized_end=305,
)


_LOOKUPPUBBYTOPICRESP = _descriptor.Descriptor(
  name='LookupPubByTopicResp',
  full_name='LookupPubByTopicResp',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='publishers', full_name='LookupPubByTopicResp.publishers', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=307,
  serialized_end=366,
)


_LOOKUPALLPUBSREQ = _descriptor.Descriptor(
  name='LookupAllPubsReq',
  full_name='LookupAllPubsReq',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=368,
  serialized_end=386,
)


_LOOKUPALLPUBSRESP = _descriptor.Descriptor(
  name='LookupAllPubsResp',
  full_name='LookupAllPubsResp',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='publishers', full_name='LookupAllPubsResp.publishers', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=388,
  serialized_end=444,
)


_DISCOVERYREQ = _descriptor.Descriptor(
  name='DiscoveryReq',
  full_name='DiscoveryReq',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='msg_type', full_name='DiscoveryReq.msg_type', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='register_req', full_name='DiscoveryReq.register_req', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='isready_req', full_name='DiscoveryReq.isready_req', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='lookup_req', full_name='DiscoveryReq.lookup_req', index=3,
      number=4, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='allpubs_req', full_name='DiscoveryReq.allpubs_req', index=4,
      number=5, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
    _descriptor.OneofDescriptor(
      name='Content', full_name='DiscoveryReq.Content',
      index=0, containing_type=None, fields=[]),
  ],
  serialized_start=447,
  serialized_end=661,
)


_DISCOVERYRESP = _descriptor.Descriptor(
  name='DiscoveryResp',
  full_name='DiscoveryResp',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='msg_type', full_name='DiscoveryResp.msg_type', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='register_resp', full_name='DiscoveryResp.register_resp', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='isready_resp', full_name='DiscoveryResp.isready_resp', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='lookup_resp', full_name='DiscoveryResp.lookup_resp', index=3,
      number=4, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='allpubs_resp', full_name='DiscoveryResp.allpubs_resp', index=4,
      number=5, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
    _descriptor.OneofDescriptor(
      name='Content', full_name='DiscoveryResp.Content',
      index=0, containing_type=None, fields=[]),
  ],
  serialized_start=664,
  serialized_end=887,
)

_REGISTERREQ.fields_by_name['role'].enum_type = _ROLE
_REGISTERREQ.fields_by_name['info'].message_type = _REGISTRANTINFO
_REGISTERRESP.fields_by_name['status'].enum_type = _STATUS
_LOOKUPPUBBYTOPICRESP.fields_by_name['publishers'].message_type = _REGISTRANTINFO
_LOOKUPALLPUBSRESP.fields_by_name['publishers'].message_type = _REGISTRANTINFO
_DISCOVERYREQ.fields_by_name['msg_type'].enum_type = _MSGTYPES
_DISCOVERYREQ.fields_by_name['register_req'].message_type = _REGISTERREQ
_DISCOVERYREQ.fields_by_name['isready_req'].message_type = _ISREADYREQ
_DISCOVERYREQ.fields_by_name['lookup_req'].message_type = _LOOKUPPUBBYTOPICREQ
_DISCOVERYREQ.fields_by_name['allpubs_req'].message_type = _LOOKUPALLPUBSREQ
_DISCOVERYREQ.oneofs_by_name['Content'].fields.append(
  _DISCOVERYREQ.fields_by_name['register_req'])
_DISCOVERYREQ.fields_by_name['register_req'].containing_oneof = _DISCOVERYREQ.oneofs_by_name['Content']
_DISCOVERYREQ.oneofs_by_name['Content'].fields.append(
  _DISCOVERYREQ.fields_by_name['isready_req'])
_DISCOVERYREQ.fields_by_name['isready_req'].containing_oneof = _DISCOVERYREQ.oneofs_by_name['Content']
_DISCOVERYREQ.oneofs_by_name['Content'].fields.append(
  _DISCOVERYREQ.fields_by_name['lookup_req'])
_DISCOVERYREQ.fields_by_name['lookup_req'].containing_oneof = _DISCOVERYREQ.oneofs_by_name['Content']
_DISCOVERYREQ.oneofs_by_name['Content'].fields.append(
  _DISCOVERYREQ.fields_by_name['allpubs_req'])
_DISCOVERYREQ.fields_by_name['allpubs_req'].containing_oneof = _DISCOVERYREQ.oneofs_by_name['Content']
_DISCOVERYRESP.fields_by_name['msg_type'].enum_type = _MSGTYPES
_DISCOVERYRESP.fields_by_name['register_resp'].message_type = _REGISTERRESP
_DISCOVERYRESP.fields_by_name['isready_resp'].message_type = _ISREADYRESP
_DISCOVERYRESP.fields_by_name['lookup_resp'].message_type = _LOOKUPPUBBYTOPICRESP
_DISCOVERYRESP.fields_by_name['allpubs_resp'].message_type = _LOOKUPALLPUBSRESP
_DISCOVERYRESP.oneofs_by_name['Content'].fields.append(
  _DISCOVERYRESP.fields_by_name['register_resp'])
_DISCOVERYRESP.fields_by_name['register_resp'].containing_oneof = _DISCOVERYRESP.oneofs_by_name['Content']
_DISCOVERYRESP.oneofs_by_name['Content'].fields.append(
  _DISCOVERYRESP.fields_by_name['isready_resp'])
_DISCOVERYRESP.fields_by_name['isready_resp'].containing_oneof = _DISCOVERYRESP.oneofs_by_name['Content']
_DISCOVERYRESP.oneofs_by_name['Content'].fields.append(
  _DISCOVERYRESP.fields_by_name['lookup_resp'])
_DISCOVERYRESP.fields_by_name['lookup_resp'].containing_oneof = _DISCOVERYRESP.oneofs_by_name['Content']
_DISCOVERYRESP.oneofs_by_name['Content'].fields.append(
  _DISCOVERYRESP.fields_by_name['allpubs_resp'])
_DISCOVERYRESP.fields_by_name['allpubs_resp'].containing_oneof = _DISCOVERYRESP.oneofs_by_name['Content']
DESCRIPTOR.message_types_by_name['RegistrantInfo'] = _REGISTRANTINFO
DESCRIPTOR.message_types_by_name['RegisterReq'] = _REGISTERREQ
DESCRIPTOR.message_types_by_name['RegisterResp'] = _REGISTERRESP
DESCRIPTOR.message_types_by_name['IsReadyReq'] = _ISREADYREQ
DESCRIPTOR.message_types_by_name['IsReadyResp'] = _ISREADYRESP
DESCRIPTOR.message_types_by_name['LookupPubByTopicReq'] = _LOOKUPPUBBYTOPICREQ
DESCRIPTOR.message_types_by_name['LookupPubByTopicResp'] = _LOOKUPPUBBYTOPICRESP
DESCRIPTOR.message_types_by_name['LookupAllPubsReq'] = _LOOKUPALLPUBSREQ
DESCRIPTOR.message_types_by_name['LookupAllPubsResp'] = _LOOKUPALLPUBSRESP
DESCRIPTOR.message_types_by_name['DiscoveryReq'] = _DISCOVERYREQ
DESCRIPTOR.message_types_by_name['DiscoveryResp'] = _DISCOVERYRESP
DESCRIPTOR.enum_types_by_name['Role'] = _ROLE
DESCRIPTOR.enum_types_by_name['Status'] = _STATUS
DESCRIPTOR.enum_types_by_name['MsgTypes'] = _MSGTYPES
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

RegistrantInfo = _reflection.GeneratedProtocolMessageType('RegistrantInfo', (_message.Message,), dict(
  DESCRIPTOR = _REGISTRANTINFO,
  __module__ = 'discovery_pb2'
  # @@protoc_insertion_point(class_scope:RegistrantInfo)
  ))
_sym_db.RegisterMessage(RegistrantInfo)

RegisterReq = _reflection.GeneratedProtocolMessageType('RegisterReq', (_message.Message,), dict(
  DESCRIPTOR = _REGISTERREQ,
  __module__ = 'discovery_pb2'
  # @@protoc_insertion_point(class_scope:RegisterReq)
  ))
_sym_db.RegisterMessage(RegisterReq)

RegisterResp = _reflection.GeneratedProtocolMessageType('RegisterResp', (_message.Message,), dict(
  DESCRIPTOR = _REGISTERRESP,
  __module__ = 'discovery_pb2'
  # @@protoc_insertion_point(class_scope:RegisterResp)
  ))
_sym_db.RegisterMessage(RegisterResp)

IsReadyReq = _reflection.GeneratedProtocolMessageType('IsReadyReq', (_message.Message,), dict(
  DESCRIPTOR = _ISREADYREQ,
  __module__ = 'discovery_pb2'
  # @@protoc_insertion_point(class_scope:IsReadyReq)
  ))
_sym_db.RegisterMessage(IsReadyReq)

IsReadyResp = _reflection.GeneratedProtocolMessageType('IsReadyResp', (_message.Message,), dict(
  DESCRIPTOR = _ISREADYRESP,
  __module__ = 'discovery_pb2'
  # @@protoc_insertion_point(class_scope:IsReadyResp)
  ))
_sym_db.RegisterMessage(IsReadyResp)

LookupPubByTopicReq = _reflection.GeneratedProtocolMessageType('LookupPubByTopicReq', (_message.Message,), dict(
  DESCRIPTOR = _LOOKUPPUBBYTOPICREQ,
  __module__ = 'discovery_pb2'
  # @@protoc_insertion_point(class_scope:LookupPubByTopicReq)
  ))
_sym_db.RegisterMessage(LookupPubByTopicReq)

LookupPubByTopicResp = _reflection.GeneratedProtocolMessageType('LookupPubByTopicResp', (_message.Message,), dict(
  DESCRIPTOR = _LOOKUPPUBBYTOPICRESP,
  __module__ = 'discovery_pb2'
  # @@protoc_insertion_point(class_scope:LookupPubByTopicResp)
  ))
_sym_db.RegisterMessage(LookupPubByTopicResp)

LookupAllPubsReq = _reflection.GeneratedProtocolMessageType('LookupAllPubsReq', (_message.Message,), dict(
  DESCRIPTOR = _LOOKUPALLPUBSREQ,
  __module__ = 'discovery_pb2'
  # @@protoc_insertion_point(class_scope:LookupAllPubsReq)
  ))
_sym_db.RegisterMessage(LookupAllPubsReq)

LookupAllPubsResp = _reflection.GeneratedProtocolMessageType('LookupAllPubsResp', (_message.Message,), dict(
  DESCRIPTOR = _LOOKUPALLPUBSRESP,
  __module__ = 'discovery_pb2'
  # @@protoc_insertion_point(class_scope:LookupAllPubsResp)
  ))
_sym_db.RegisterMessage(LookupAllPubsResp)

DiscoveryReq = _reflection.GeneratedProtocolMessageType('DiscoveryReq', (_message.Message,), dict(
  DESCRIPTOR = _DISCOVERYREQ,
  __module__ = 'discovery_pb2'
  # @@protoc_insertion_point(class_scope:DiscoveryReq)
  ))
_sym_db.RegisterMessage(DiscoveryReq)

DiscoveryResp = _reflection.GeneratedProtocolMessageType('DiscoveryResp', (_message.Message,), dict(
  DESCRIPTOR = _DISCOVERYRESP,
  __module__ = 'discovery_pb2'
  # @@protoc_insertion_point(class_scope:DiscoveryResp)
  ))
_sym_db.RegisterMessage(DiscoveryResp)


# @@protoc_insertion_point(module_scope)
