import collections
import string
from os.path import commonprefix

import parse

FORMATTER = string.Formatter()


class Stringable(object):
    def __init__(self, name, template, valid_values={}):
        self.template_pieces = []
        self.field_names = []
        parsed = FORMATTER.parse(template)
        for (literal_text, field_name, format_spec, conversion) in parsed:
            assert not conversion
            self.template_pieces.append((literal_text, field_name))
            if field_name not in self.field_names:
                self.field_names.append(field_name)

        self.name = name
        self.template = template
        self.compiled_template = parse.compile(template)
        self.tuple_class = collections.namedtuple(
            self.name,
            list(self.field_names))

        self.valid_values = dict(valid_values)
        for key in self.valid_values:
            assert key in self.field_names

    def make_tuple(self, string_value=None, **kwargs):
        if string_value is not None:
            assert not kwargs
            parsed = self.compiled_template.parse(string_value)
            if parsed is None:
                raise ValueError(
                    "Stringable [%s]: Couldn't parse '%s' according to "
                    "template '%s'" % (
                        self.name, string_value, self.template))
            assert not parsed.fixed
            fields_dict = parsed.named
        else:
            fields_dict = kwargs
        self.check_fields(**fields_dict)
        return self.tuple_class(**fields_dict)

    def check_fields(self, **fields_dict):
        assert set(fields_dict) == set(self.field_names), (
            "%s: passed fields %s != expected fields %s" % (
                self.name, set(fields_dict), set(self.field_names)))
        for (key, values) in self.valid_values.items():
            if fields_dict[key] not in values:
                raise RuntimeError(
                    "Invalid value %s='%s', must be one of %s." % (
                        key, fields_dict[key], ', '.join(values)))

    def make_string(self, tpl=None, **fields_dict):
        if tpl is not None:
            assert not fields_dict
            fields_dict = tpl._asdict()
        self.check_fields(**fields_dict)
        return self.template.format(**fields_dict)

    def prefix(self, **fields_dict):
        (prefix,) = self.prefixes(**fields_dict)
        return prefix

    def prefixes(self, max_prefixes=1, **fields_dict):
        for (key, value) in fields_dict.items():
            assert key in self.field_names, key
            assert value is None or isinstance(value, list), type(value)

        def make_prefixes(
                template_pieces,
                max_prefixes=max_prefixes,
                fields_dict=fields_dict):
            result = [[]]
            if not template_pieces:
                return result

            (literal, field_name) = template_pieces[0]
            if literal:
                for piece in result:
                    piece.append(literal)

            values = fields_dict.get(field_name)
            if values is None:
                values = self.valid_values.get(field_name)
            if values is not None:
                if len(result) * len(values) > max_prefixes:
                    common_prefix = commonprefix(values)
                    for piece in result:
                        piece.append(common_prefix)
                else:
                    new_result = []
                    for value in values:
                        new_fields_dict = dict(fields_dict)
                        new_fields_dict[field_name] = [value]
                        rest = make_prefixes(
                            template_pieces[1:],
                            max_prefixes=max_prefixes / (
                                len(result) * len(values)),
                            fields_dict=new_fields_dict)
                        for some_rest in rest:
                            new_result.extend(
                                [x + [value] + some_rest for x in result])
                    result = new_result
            return result

        prefix_components = make_prefixes(self.template_pieces)
        assert len(prefix_components) <= max_prefixes
        return [''.join(x) for x in prefix_components]
