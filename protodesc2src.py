#!/usr/bin/env python3

import argparse
import functools
import os
import sys
from google.protobuf import descriptor_pb2


def remove_prefix(string, prefix):
    if string.startswith(prefix):
        return string[len(prefix):]
    else:
        return string


def Indent(fun):
    @functools.wraps(fun)
    def wrapped(self, *args, **kwargs):
        self.indent_level += 1
        result = fun(self, *args, **kwargs)
        self.indent_level -= 1
        return result

    return wrapped


class ProtoWriter:
    def __init__(self, indent=4):
        if type(indent) == int:
            self.indent = ' ' * indent
        elif type(indent) == str:
            self.indent = indent
        else:
            raise ValueError("Invalid indent value")

    def __call__(self, desc, out):
        self.out = out
        self.indent_level = 0

        if not desc.syntax:
            desc.syntax = 'proto2'

        self.write_stmt(f'syntax = "{desc.syntax}"')
        if desc.package:
            self.package_name = desc.package
            self.write_stmt(f'package {desc.package}')

        self.out.write('\n')

        if desc.options:
            self.write_file_options(desc.options)
            self.out.write('\n')

        self.write_dependency(desc.dependency)
        for enum in desc.enum_type:
            self.write_enum_type(enum)

        for msg in desc.message_type:
            self.write_message_type(msg)

    def get_option_value(self, value):
        if type(value) == str:
            return f'"{value}"'
        if type(value) == bool:
            return 'true' if value else 'false'

    def write_file_options(self, options):
        for field, value in options.ListFields():
            if field.name == 'optimize_for':
                value = descriptor_pb2.FileOptions.OptimizeMode.Name(value)
            else:
                value = self.get_option_value(value)
            self.write_stmt(f'option {field.name} = {value}')

    def write_dependency(self, deps):
        for dep in deps:
            self.write_stmt(f'import "{dep}"')
        if len(deps):
            self.out.write('\n')

    def write_enum_type(self, enum):
        self.write_ln(f'enum {enum.name} {{')
        self.write_enum_body(enum)
        self.write_ln('}')

    @Indent
    def write_enum_body(self, enum):
        for value in enum.value:
            self.write_stmt(f'{value.name} = {value.number}')

    def write_message_type(self, message):
        self.write_ln(f'message {message.name} {{')
        self.write_message_body(message)
        self.write_ln('}')

    @Indent
    def write_message_body(self, message):
        for field in message.field:
            self.write_message_field(message, field)
        for nested_type in message.nested_type:
            if nested_type.options.map_entry:
                continue
            self.write_message_type(nested_type)
        for enum_type in message.enum_type:
            self.write_enum_type(enum_type)

    def write_message_field(self, message, field):
        self.write_indent()
        self.write_field_type(message, field)
        self.write(f' {field.name} = {field.number}')
        if field.default_value:
            raise NotImplementedError("field default_value")
        if field.extendee:
            raise NotImplementedError("field extendee")
        if field.options:
            self.write_field_options(field.options)
        self.write(';\n')

    def write_ln(self, s):
        self.write_indent()
        self.write(s)
        self.write('\n')

    def write_stmt(self, s):
        self.write_indent()
        self.write(s)
        self.write(';\n')

    def write_indent(self):
        self.write(self.indent * self.indent_level)

    def write(self, s):
        self.out.write(s)

    def write_field_type(self, message, field):
        type = descriptor_pb2.FieldDescriptorProto.Type.Name(field.type)
        type_name = self.get_field_type_name(field)

        nested = False
        if type == 'TYPE_MESSAGE' or type == 'TYPE_ENUM':
            nested = type_name.startswith(message.name + '.')

        if nested:
            type_name = type_name[len(message.name) + 1:]

        if nested and type == 'TYPE_MESSAGE':
            nested_type = self.get_nested_type(message, type_name)
            if nested_type.options.map_entry:
                key_type = self.get_field(nested_type, 'key')
                value_type = self.get_field(nested_type, 'value')
                type_name = f'map<{self.get_field_type_name(key_type)}, {self.get_field_type_name(value_type)}>'
                self.out.write(type_name)
                return

        if field.label:
            self.write_field_label(field.label)
        self.write(type_name)

    def get_field_type_name(self, field):
        name = descriptor_pb2.FieldDescriptorProto.Type.Name(field.type)
        name_map = {
            'TYPE_BOOL': 'bool',
            'TYPE_BYTES': 'bytes',
            'TYPE_DOUBLE': 'double',
            'TYPE_INT32': 'int32',
            'TYPE_INT64': 'int64',
            'TYPE_UINT32': 'uint32',
            'TYPE_UINT64': 'uint64',
            'TYPE_SINT32': 'sint32',
            'TYPE_SINT64': 'sint64',
            'TYPE_FIXED32': 'fixed32',
            'TYPE_FIXED64': 'fixed64',
            'TYPE_SFIXED32': 'sfixed32',
            'TYPE_SFIXED64': 'sfixed64',
            'TYPE_STRING': 'string',
        }
        if name in name_map:
            return name_map[name]
        if name == 'TYPE_MESSAGE' or name == 'TYPE_ENUM':
            type_name = field.type_name.lstrip('.')
            type_name = remove_prefix(type_name, self.package_name + '.')
            return type_name
        else:
            raise NotImplementedError(name)

    def get_nested_type(self, message, name):
        for nested_type in message.nested_type:
            if nested_type.name == name:
                return nested_type
        raise LookupError("No nested type with name " + name)

    def get_field(self, messageOrType, name):
        for field in messageOrType.field:
            if field.name == name:
                return field

    def write_field_options(self, options):
        for option, value in options.ListFields():
            value = self.get_option_value(value)
            self.write(f' [{option.name} = {value}]')

    def write_field_label(self, label):
        raise NotImplementedError('write_field_label')


class Desc2Proto2(ProtoWriter):
    def write_field_label(self, label):
        label = descriptor_pb2.FieldDescriptorProto.Label.Name(label)
        if label == 'LABEL_REPEATED':
            self.write('repeated ')
        elif label == 'LABEL_OPTIONAL':
            self.write('optional ')
        elif label == 'LABEL_REQUIRED':
            self.write('required ')
        else:
            raise RuntimeError(f'Unknown label {label}')


class Desc2Proto3(ProtoWriter):
    def write_field_label(self, label):
        label = descriptor_pb2.FieldDescriptorProto.Label.Name(label)
        if label == 'LABEL_REPEATED':
            self.write('repeated ')
        elif label == 'LABEL_OPTIONAL':
            pass
        else:
            raise RuntimeError(f'Invalid label {label} in proto3 descriptor')


def desc2proto(desc, out):
    if desc.syntax and desc.syntax == 'proto3':
        conv = Desc2Proto3()
    else:
        conv = Desc2Proto2()
    conv(desc, out)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('file', help='Path to serialized file descriptor')
    parser.add_argument('out', nargs='?',
                        help='Output file name.'
                             ' Omit to write to the filename specified in the file descriptor.'
                             ' Use "-" to write to stdout')
    args = parser.parse_args()

    with open(args.file, 'rb') as f:
        pb = f.read()

    desc = descriptor_pb2.FileDescriptorProto()
    desc.ParseFromString(pb)

    if not desc.name:
        sys.exit("Invalid file descriptor: No name")

    if not args.out:
        out_path = desc.name.split('/')[-1]
    elif args.out == '-':
        out_path = None
    else:
        out_path = args.out

    if out_path and os.path.exists(out_path):
        sys.exit(f"Output path {out_path} exists. Refusing to overwrite.")

    if out_path:
        with open(out_path, 'w') as f:
            desc2proto(desc, f)
        print(f"Wrote output to {out_path}")
    else:
        desc2proto(desc, sys.stdout)
