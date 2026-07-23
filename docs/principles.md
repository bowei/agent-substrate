# General principles

These are some high-level descriptions of how we view this project in relation
to others. 

These principles will change over time as we get more understanding on the
shape of the problem:

## Substrate and dependencies

We should make Substrate self-contained as much as possible. There will always
have dependencies (Linux, K8s, Golang, uVMs etc) but we want to consume them
as released artifacts rather than co-developed software. A co-developed
dependency is one where patches need to be merged simultaneously across
multiple projects/orgs to implement a feature. This introduces a lot of
complexity: even creating this kind of dependency for repos within the same
org will generate a lot of friction.

There is a subtlety here: we know that dependencies will evolve and as
Substrate gets more traction, they build features for Substrate explicitly.
This is a good thing! Substrate should drive changes and take advantage of
them. The key concern is that the primary implementation and logic for a
Substrate feature cannot live entirely outside in a different project.

## Batteries included and extension points

Substrate project must ship a "batteries included" experience that works for
production and be feature-complete. For the project to be successful, this
should be able to run real workloads out-of-the-box. This means that we will
overlap with other existing pieces of software/vendor products. This is
expected and required. We don't want to unnecessarily rebuild things but a key
opportunity is the novel design and layering to support Agentic workloads.

While the layers in the system are being discovered, we should be reasonably
disciplined about describing the surface area of the interfaces we consume.

We know that users will replace portions of Substrate to specialize their
deployments: get more features, better performance, integrate into a managed
offer,  etc. To this end, the project preference is for the extension points to
fall naturally out of good system design. Monoliths are bad design and we
should encourage component boundaries to emerge. As the project matures
(post-V1), these will graduate to contracts with guarantees but we don't need
to force it unnecessarily at this point in time.

