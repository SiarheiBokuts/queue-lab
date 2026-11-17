// npm audit with a list of advisories we accept. Run in a folder with package.json and package-lock.json.
// Fails on any high or critical advisory that is not in IGNORED; every ignored one is printed with its reason.
import { spawnSync } from "node:child_process";

// Remove an entry as soon as a patched version exists.
const IGNORED = {
  "GHSA-fc5q-39xc-vjpf":
    "picomatch ReDoS (CVE-2026-112366), no patched version yet; only the build uses it (vue-router, vite), not the browser",
};

const result = spawnSync("npm", ["audit", "--omit=dev", "--json"], { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
const report = JSON.parse(result.stdout);

// Every advisory is an object inside "via"; a string there only points to another package.
const advisories = new Map();
for (const vulnerability of Object.values(report.vulnerabilities ?? {})) {
  for (const via of vulnerability.via) {
    if (typeof via === "object") advisories.set(via.url, via);
  }
}

let failed = false;
for (const [url, advisory] of advisories) {
  const id = url.split("/").pop();
  if (id in IGNORED) {
    console.log(`ignored  ${id}  ${advisory.name}: ${IGNORED[id]}`);
  } else if (advisory.severity === "high" || advisory.severity === "critical") {
    console.log(`FAILED   ${id}  ${advisory.name} (${advisory.severity}): ${advisory.title}  ${url}`);
    failed = true;
  }
}
console.log(failed ? "npm audit: failed" : "npm audit: nothing above the level 'high' except the ignored advisories");
process.exit(failed ? 1 : 0);
