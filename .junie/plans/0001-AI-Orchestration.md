# AI Orchestration

## Introduction

Because the job of a software developer has recently
drastically shifted from writing code to writing essays,
and because I have a pretty vague idea of how the
development process that I aim to implement will look
like, and because I'm not familiar with any 
"best-practice" projects where it's already set up, and
I don't want to give the initiative in such an important
architectural decision to the AI agents themselves,
I'm starting this essay.

The goal, however, is not writing itself. The goal is
to set up a nice, effective, transparent, cheap,
flexible, extendable process for the development
of [my-handicapped-pet.io](https://my-handicapped-pet.io),
which can also be adapted for other projects of similar
size.

Because I already had some problems with interpretation
of the documents written for both humans and AI agents,
I feel a necessity to clarify prepositions,
like in legal documents. "I" means me, Ilya L.,
the solo (as for 2026-09-30) developer of this project.
"You", if encountered anywhere in the text, likely
means the metaphorical Socratic conversation peer,
but can be defaulted to any individual reader,
never mind human or bot.

However, if you are an AI agent, editing this document,
please follow the following guidelines:
 - edit only sections which title specifically says
*Written by the AI Agent*;
 - keep nice 60-character-per-line formatting,
unless for long links or stuff styled as `code`;
 - be high-level. Implementation details can be further
clarified in the tasks.

Besides the necessity, this writing is inspired
by a few Internet posts, among them 
[Chad Arimura's blog post](https://chad.cm/posts/2026-8-11-my-agent-setup)
that I encountered on Hacker News, and the diary
of [Nancy Sadkov](https://lj.rossia.org/users/nancygold/)
who writes, besides her magnificent life experience,
about the revival of her old software projects with the
power of the modern agentic coding.

## Current Setup and Requirements

Currently I use an old-school way of doing agentic coding:
open chat in my IDE, ask the AI agent to write
requirements for a new feature or change request,
or to analyze and edit a document I've already started.
Then, after we've got detailed enough requirements,
I ask to write the code, to test it internally with unit
tests, then I test it end-to-end (normally, manually),
deploy to the staging and then to the prod.

While this process speeds up development significantly,
it leaves clear room for improvement. I'll list 
one-by-one items that I wish to improve, and also
common requirements.

1. The project consists of several repos (the full list
is within GitHub 
[my-handicapped-pet](https://github.com/orgs/my-handicapped-pet/repositories)
organization). AI agent within the IDE is scoped to 
a single IDE's project, therefore, to a single repo. 
The obvious solution is to open the IDE with the root
that contains all repos, but it's not necessarily
the best. The goal is to keep interfaces between
the parts of the project (e.g. between backend
and frontend) clear i.e. minimalistic in terms 
of technical design and effective in terms of runtime.
So, it's probably better to keep a separate agent for
each repo, and allow them to communicate with each other.
2. Planning. Currently, planning is started by me with
a high-level or sometimes more detailed proposition,
then I together with the AI agent update the docs
back and forth recursively. Basically, this process
is synchronous and requires human attendance. I still
want a human to be mandatory in the workflow before
implementation, but maybe introducing another agent
can reduce the number of iterations and give the
higher quality design for the human review.
Then, the reviewer can accept the design, criticize it,
or amend or rewrite it manually.
3. Marketing. This might be not an obvious part
of the development workflow, but AI agents can search
the web, find similar projects (competitors, or
projects to merge/integrate with), potential users,
potential use cases, potential places 
to promote the project. Also, the marketing agent
may suggest features to the planning agent.
4. Durability. Asynchronous and independent agentic work 
will require hosting agents in the cloud; the developer
workstation must be removed from the loop. So, we'll
need some service or solution to host the agents.
5. Messaging. The AI agents must be able to communicate
to each other; the developers must be able to read their
communications and communicate to them; the AI agents
must be able to summon the developer when they need
human attention. Therefore, we need to set up 
some bot-friendly messenger.
6. Documentation. After the feature is implemented, its
documentation must follow. A bunch of MD files is
technically OK, but in the current structure it is
just a task log, not organized around the project 
structure.
7. Price. This is a non-commercial pet project. 
Currently it costs me about $30 per month for the AWS
hosting. If AI agent infrastructure adds some expenses,
they should be the same order of magnitude.
8. Deployments. Currently deployments are triggered
by pushing to the `develop` branch. We can keep this
logic, if the AI agents are able to make pull requests.
9. Testing. After deployment to the staging, we can
trigger a specific testing AI agent, that plans and
executes end-to-end tests. A more sophisticated
alternative is for the agents to create a temporary
dev environment in their own cloud.

The first implementation doesn't necessarily need to implement
all requirements listed above. We can start with some
reasonable setup that implements a part of them.

## Implementation Proposals (Written by the AI Agent)

### Proposal 1: GitHub-Native Orchestration

The first implementation should use GitHub as the
durable coordination layer and Junie CLI as the main
agent runtime. GitHub should keep the source of truth:
issues, pull requests, labels, comments, checklists,
branch protection, and Actions logs. Junie should do
the actual planning, implementation, review, and QA work
inside repo-scoped runs.

This means the backbone is not a custom always-on agent
service at first. It is a combination of GitHub-native
workflow objects, local or headless Junie invocations,
and deterministic command tools. The agent should be
self-sufficient inside its repo scope: after it starts,
it should be able to call pre-flight checks, GitHub
commands, branch helpers, and GitHub Actions workflows
as tools. Workflows should normally support the agent,
not be the only thing that starts or controls it.

Each run should be scoped to one repo or one planning
artifact. Cross-repo work should happen through explicit
issue links and interface notes, not by letting one
agent freely edit every repository at once.

The recommended first set of agents is:

- `planner-agent`: expands a high-level idea into goals,
  non-goals, affected repos, interface changes,
  acceptance criteria, test strategy, and rollout notes.
- `repo-agent`: implements an approved task inside a
  single repository and opens a pull request.
- `review-agent`: reviews pull requests for correctness,
  tests, documentation, and boundary violations.
- `qa-agent`: runs or designs staging checks after a
  deployment and posts a concise report.
- `marketing-agent`: researches users, competitors,
  integration opportunities, and feature ideas, but does
  not create implementation work without human triage.

GitHub-native objects should define the protocol between
agents. An issue describes the requested change and its
state. Labels route work between roles, for example
`agent:planning`, `ready-for-implementation`,
`agent:backend`, `agent:frontend`, `qa-needed`, and
`needs-human`. Pull requests carry implementation and
review context. Checklists define the minimum acceptance
criteria. GitHub Actions provide repeatable checks,
deployments, and optional headless execution targets that
an agent can trigger when it needs remote work.

Junie-specific project configuration can live in each
repository. `AGENTS.md` or `.junie/AGENTS.md` should hold
repo-specific rules. `.junie/skills/` can hold reusable
checklists and conventions. `.junie/agents/` can define
custom subagents for planning, review, QA, docs, and
marketing. MCP configuration can add GitHub access and,
later, browser or web-search tools for QA and marketing.

The practical first milestone should prove a local,
manually invoked agent workflow inside the future
coordination repo. It should not invoke agents in other
repositories yet, because those repositories are not enrolled
and do not have their own `repo-agent` configuration. At the
same time, it should include enough Git and GitHub interaction
to be more than two chats with different prompts:

1. Choose a coordination repo. It should store shared
   agent conventions and the local command-center notes.
   This repo provides the command center; the
   `planner-agent` is only one role invoked by it.
2. Define the minimum local `planner-agent` convention
   needed to run Junie CLI by hand. The first version can
   use Markdown prompts or notes instead of a complete
   `.junie/agents/` structure.
3. Add a tiny local command or documented command sequence
   for the first useful mechanics: pre-flight checks,
   creating a branch in the coordination repo, preserving
   the plan as a file, and preparing or opening a pull
   request for that plan.
4. Run the `planner-agent` locally against a planning
   document or issue text and produce a human-reviewable
   implementation plan in the coordination repo.
5. Let the `planner-agent`, through the local command
   sequence, update the coordination repo branch and pull
   request that contains the plan. The agent may also record
   the target application repo and proposed handoff, but it
   must not run a `repo-agent` in that target repo yet.
6. Record the minimal handoff format that will later be used
   by an enrolled target repo: plan link or file, affected
   repo, acceptance criteria, test strategy, expected branch
   or PR shape, and final status summary.
7. Defer target-repo `repo-agent` execution, issue
   templates, pull request templates, routing labels,
   GitHub Actions workflows, generic GitHub helper scripts,
   status aggregation, chatbot integration, label-triggered
   execution, cross-repo dispatching, and repo enrollment
   until the local coordination workflow is clear.

The second milestone can add automation:

1. Add issue templates, a pull request checklist,
   routing labels, and repo-level agent guidelines.
   The labels should make it clear when work is waiting
   for planning, implementation, review, QA, or human
   attention.
2. Add a small layer to interact with GitHub objects.
   All agent commands should use this layer instead of
   scattering raw GitHub API, `gh`, GraphQL, or MCP calls
   through prompts and workflows. The layer should expose
   project-level operations such as `create-plan-issue`,
   `post-agent-report`, `create-repo-task`, `create-pr`,
   `mark-needs-human`, and `get-agent-status`.
3. Add manual CLI entry points for the fuller workflow:
   create or update a planning issue, run the
   `planner-agent`, create repo-specific tasks, run a
   `repo-agent` in an enrolled target repo, create a pull
   request, and collect the current status. These commands
   can later be mapped to Junie slash commands, GitHub
   comments, or a chatbot.
4. Decide where agent roles live. The coordination repo
   should hold the `planner-agent`, `marketing-agent`,
   shared review rules, QA orchestration, shared scripts,
   and enrollment commands. Each application repo should
   hold its own `repo-agent` configuration, repo-specific
   guidelines, test commands, and minimal workflow files.
5. Add `enroll-repo` and `enroll-all-repos` commands.
   These commands should be invoked from the coordination
   repo, clone or check out the target repo, apply the
   standard agent files, create a branch, and open a pull
   request in that target repo. They should not push
   directly to the default branch.
6. Add GitHub Actions workflows as tools that agents can
   trigger for remote work: tests, deployments, QA runs,
   enrollment updates, or optional headless Junie tasks.
   Start with explicit `workflow_dispatch` calls made by
   the command layer, then add label-triggered or
   comment-triggered shortcuts only after permissions and
   failure modes are understood.
7. Add cross-repo dispatching. A planning issue in the
   coordination repo should be able to create linked
   implementation issues or workflow runs in the affected
   application repos.
8. Add richer status aggregation across all open plans,
   related issues, pull requests, workflow runs, staging
   checks, and items waiting for human attention.
9. Add chatbot or messenger slash-command mapping only as
   an interface over the same deterministic CLI commands.

Old version:

1. Add a planning issue template.
2. Add a cross-repo interface-note template.
3. Add a PR checklist for agent-made changes.
4. Add routing labels for agent roles and human review.
5. Add repo-level agent guidelines.
6. Run agents manually or semi-manually at first.
7. Add GitHub Actions triggers only after the process is
   clear enough to automate safely.

Useful references:

- [GitHub Issues documentation](https://docs.github.com/en/issues/tracking-your-work-with-issues/about-issues)
- [GitHub pull request documentation](https://docs.github.com/en/pull-requests)
- [GitHub Actions documentation](https://docs.github.com/en/actions)
- [GitHub Actions workflow syntax](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax)
- [GitHub labels documentation](https://docs.github.com/en/issues/using-labels-and-milestones-to-track-work/managing-labels)
- [GitHub branch protection rules](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches)
- [GitHub CLI manual](https://cli.github.com/manual/)
- [Model Context Protocol documentation](https://modelcontextprotocol.io/docs/getting-started/intro)
- [AGENTS.md format](https://agents.md/)
- [Agent Skills specification](https://agentskills.io/specification)
- [Junie CLI](https://www.jetbrains.com/junie/)

## Acceptance Criteria

For milestone #1:
 - `planner-agent`, running interactively, can read or
   edit this very document according to the guidelines;
 - `repo-agent`, running interactively in the coordination
   repo, can implement branch creation and pull request
   creation for coordination-repo planning changes;
 - `repo-agent` also implements the local command sequence
   that performs pre-flight checks before the agent does
   Git or GitHub work;
 - both agents, running in the coordination repo, can
   update the branch and the pull request;

For milestone #2:
 - `planner-agent`, running interactively from this
   repo, can enroll other repos by creating pull
   requests;
 - I accept these pull requests;
 - in `planner-agent`, running interactively from this
   repo, I ask, for the notification feature, implemented
   recently in the backend, implement frontend;
 - `planner-agent`, through the command layer, checks out
   the frontend repo with the correct branch, creates a
   plan, pushes it, creates a pull request for the
   frontend repo, and creates a ticket in the frontend
   repo pointing to that plan with correct labels;
 - by interactive communication with `planner-agent`,
   it can update the pull request;
 - by commenting the ticket or the pull request,
   `planner-agent` can trigger the required remote
   workflow or headless agent command and update the
   pull request;
 - once the ticket labeled as approved, `repo-agent`,
   through the command layer in the frontend repo, can
   trigger remote checks or a headless update and update
   the pull request with the implementation;
 - by commenting the ticket or the pull request,
   `repo-agent` can trigger the required remote workflow
   or headless agent command and update the implementation
   in the pull request;
 - `repo-agent`, running interactively, can
   update the implementation in the pull request.
