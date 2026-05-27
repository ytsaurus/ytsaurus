# Draft-0: About the Draft Category

## Purpose

The **Draft** category is a playground for writing and editing documentation articles, by developers, AI agents and contributors in early-stage exploration.

Articles in this category are **work-in-progress**. Content may be temporarily inconsistent, incomplete, or inaccurate. Do not rely solely on draft articles for production decisions.

## Rules for Draft Articles

1. **Numbering.** Drafts are consecutively numbered starting from `draft-0`. Numbers are never reused, even if an article is removed or promoted.

2. **TOC registration.** Every draft article must be listed in `yt/docs/en/toc.yaml` and/or `yt/docs/ru/toc.yaml`, and draft entries there must appear in draft-number order.

3. **Single language.** Each draft article is written in a single language (English or Russian). Translations are only added after the article reaches its final edited state and is graduated out of Draft.

4. **No stability guarantee.** Draft content can change significantly at any time — including structural rewrites, removal of sections, or renaming. Readers should treat all drafts as volatile.

5. **AI-assisted authoring.** Drafts may be created or substantially edited by AI agents. Human review is expected before graduation.

6. **Graduation process.** When a draft article is ready for production, it is moved to the appropriate documentation category and a proper translation/review cycle begins. The original draft entry in this category could be removed immediately or replaced with a redirect or a short tombstone note indicating where the content moved.

7. **Scope.** Draft articles may cover any topic relevant to {{product-name}}: new features, architecture explorations, operational how-tos, or experimental ideas. There is no constraint on subject matter within this category.

8. **Self-contained.** Each draft should be understandable on its own. Avoid hard dependencies on other draft articles that may themselves be unstable.

9. **Review encouraged.** Anyone — human or AI — is encouraged to leave comments and suggest improvements on draft articles. Draft PRs benefit from lightweight review focused on factual correctness rather than style.

10. **Metadata header.** Each draft article should begin or end with a short metadata block (in a comment or as a leading paragraph) noting: the draft number, the author or agent that created it, the creation date, and the current status (e.g., *in progress*, *ready for review*, *ready for graduation*).

## This Article

- **Draft number:** 0
- **Author:** AI agent (GitHub Copilot)
- **Created:** 2026-05-27
- **Status:** Final — this article is the permanent description of the Draft category and is not subject to graduation.
