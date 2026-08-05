#!/usr/bin/env python3
import argparse
import os
import xml.etree.ElementTree as ET


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert JaCoCo XML report to LCOV format")
    parser.add_argument("--xml", required=True, help="Path to jacocoTestReport.xml")
    parser.add_argument("--out", required=True, help="Path to output lcov.info")
    parser.add_argument("--root", required=True, help="Project root")
    parser.add_argument("--module", action="append", default=[], help="Module roots with src/main/java")
    return parser.parse_args()


def find_source_path(modules: list[str], package_name: str, source_file: str) -> str | None:
    package_path = package_name.replace(".", "/") if "." in package_name else package_name
    rel = os.path.join(package_path, source_file) if package_path else source_file

    for module in modules:
        candidate = os.path.join(module, "src", "main", "java", rel)
        if os.path.exists(candidate):
            return os.path.abspath(candidate)

    return None


def convert(xml_path: str, lcov_path: str, modules: list[str]) -> None:
    tree = ET.parse(xml_path)
    root = tree.getroot()

    records: list[str] = []
    for package in root.findall("package"):
        package_name = package.attrib.get("name", "")

        for source in package.findall("sourcefile"):
            source_name = source.attrib.get("name")
            if not source_name:
                continue

            source_path = find_source_path(modules, package_name, source_name)
            if source_path is None:
                continue

            line_entries = source.findall("line")
            if not line_entries:
                continue

            lf = 0
            lh = 0
            body = ["TN:", f"SF:{source_path}"]

            for line in line_entries:
                nr = int(line.attrib.get("nr", "0"))
                ci = int(line.attrib.get("ci", "0"))
                if nr <= 0:
                    continue
                lf += 1
                if ci > 0:
                    lh += 1
                body.append(f"DA:{nr},{ci}")

            body.append(f"LF:{lf}")
            body.append(f"LH:{lh}")
            body.append("end_of_record")
            records.extend(body)

    with open(lcov_path, "w", encoding="utf-8") as out:
        out.write("\n".join(records))
        out.write("\n")


def main() -> None:
    args = parse_args()
    modules = args.module
    if not modules:
        modules = [args.root]
    convert(args.xml, args.out, modules)


if __name__ == "__main__":
    main()
