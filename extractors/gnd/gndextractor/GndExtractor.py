from lxml import etree
from gzip import GzipFile

from gndextractor.utils import consoleutils
from propertymapping import property_mapping


class GndExtractor:
    XML_ENTITIES_PATH = "ns:collection/ns:record"
    XML_NAMESPACES = {
        "ns": "http://www.loc.gov/MARC21/slim"
    }

    def __init__(self, db_writer, is_quiet=False):
        self.db_writer = db_writer
        self.is_quiet = is_quiet
        self.property_mapping = property_mapping
        self.namespaces = self.XML_NAMESPACES
        self.entities_path = self.apply_namespaces(self.XML_ENTITIES_PATH)

    def apply_namespaces(self, element_path):
        if self.namespaces:
            nodes = []
            for node in element_path.split("/"):
                for prefix, namespace in self.namespaces.iteritems():
                    node = node.replace("{0}:".format(prefix),
                                        "{{{0}}}".format(namespace))
                nodes.append(node)

            return "/".join(nodes)
        else:
            return element_path

    def process_dump(self, dump_id, dump_file):
        node_path = []
        uncompressed_dump_file = GzipFile(mode="rb", fileobj=dump_file)
        for event, element in etree.iterparse(uncompressed_dump_file, events=("start", "end")):
            if event == "start":
                node_path.append(element.tag)
            if event == "end":
                if "/".join(node_path) == self.entities_path:
                    for table_name, row in self.process_entity(dump_id, element):
                        self.db_writer.write_row(table_name, row)
                    self.clean_up_references(element)

                elif not "/".join(node_path).startswith(self.entities_path):
                    self.clean_up_references(element)

                del node_path[-1]
                if not self.is_quiet:
                    message = "Processing database dump...{0}"
                    consoleutils.print_progress(message, dump_file.tell())

        # Write new line to console to overwrite progress
        if not self.is_quiet:
            print

        uncompressed_dump_file.close()

    @staticmethod
    def clean_up_references(element):
        # Clean up unneeded references
        # http://www.ibm.com/developerworks/xml/library/x-hiperfparse/
        element.clear()
        if element.getparent() is not None:
            del element.getparent()[0]

    def process_entity(self, dump_id, entity_element):
        for table_name, scopes in self.property_mapping[dump_id].iteritems():
            for scope_path, columns in scopes.iteritems():
                if scope_path == ".":
                    scope_elements = [entity_element]
                else:
                    scope_elements = entity_element.xpath(scope_path, namespaces=self.namespaces)
                if scope_elements is not None:
                    for scope_element in scope_elements:
                        row = self.process_entity_scope(entity_element, scope_element, columns)
                        if row:
                            yield table_name, row

    def process_entity_scope(self, entity_element, scope_element, columns):
        row = {}
        for column, properties in columns.iteritems():
            if properties["scope"] == "global":
                affected_element = entity_element
            else:
                affected_element = scope_element
            value = self.apply_xpath(affected_element, properties["xpath"])
            if value:
                row[column] = value

        return row

    def apply_xpath(self, element, xpath):
        result = element.xpath(xpath, namespaces=self.namespaces)

        if isinstance(result, list) and len(result) > 0:
            result = result[0]

        if isinstance(result, etree._Element):
            return unicode(result.text, encoding="utf-8", errors='ignore')
        elif isinstance(result, str):
            return unicode(result, encoding="utf-8", errors='ignore')
        elif isinstance(result, unicode):
            return result
