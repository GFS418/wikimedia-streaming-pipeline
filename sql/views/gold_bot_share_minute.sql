-- Decision D20: bot share per minute across all wikis, as a plain view over gold, so there
-- is no second streaming aggregation or state. Measured over edits and new pages only.
CREATE OR REPLACE VIEW workspace.wikistream.gold_bot_share_minute
COMMENT 'Bot share of edits + new pages per minute of event time, all wikis (decision D20).'
AS
SELECT
  window_start,
  window_end,
  sum(bot_changes) AS bot_changes,
  sum(human_changes) AS human_changes,
  sum(bot_changes) / nullif(sum(bot_changes) + sum(human_changes), 0) AS bot_share
FROM workspace.wikistream.gold_wiki_minute
GROUP BY window_start, window_end
