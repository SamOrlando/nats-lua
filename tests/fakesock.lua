-- Copyright (c) 2023-2026 Sam Orlando
-- SPDX-License-Identifier: MIT (see LICENSE)
-- Fake LuaSocket-compatible TCP object used by the unit tests and benchmarks.
-- Mimics receive(pattern, prefix) semantics: '*l' strips CR, numeric patterns
-- count the prefix, and a short read returns nil, 'timeout', partial.
local M = {}
local Sock = {}
Sock.__index = Sock

function M.new(data)
    return setmetatable({ inbuf = data or "", pos = 1, sent = {}, timeout = nil,
        closed = false, eof = false }, Sock)
end

function Sock:feed(s)
    self.inbuf = self.inbuf:sub(self.pos) .. s
    self.pos = 1
end

function Sock:settimeout(t) self.timeout = t; return 1 end
function Sock:gettimeout() return self.timeout end
function Sock:setoption() return 1 end
function Sock:shutdown() self.closed = true; return 1 end
function Sock:close() self.closed = true; return 1 end

function Sock:written()
    local s = table.concat(self.sent)
    return s
end

function Sock:clear() self.sent = {} end

function Sock:send(data, i, j)
    if self.closed then return nil, "closed", (i or 1) - 1 end
    i = i or 1
    local s = data:sub(i, j)
    self.sent[#self.sent + 1] = s
    if self.on_send then self.on_send(self, s) end
    return j or #data
end

local function short(self, prefix)
    if self.on_empty then
        local more = self.on_empty(self)
        if more then
            self:feed(more)
            return nil
        end
    end
    local p = self.inbuf:sub(self.pos)
    self.pos = #self.inbuf + 1
    return nil, (self.eof or self.closed) and "closed" or "timeout", prefix .. p
end

function Sock:receive(pat, prefix)
    prefix = prefix or ""
    while true do
        local avail = self.inbuf
        if pat == "*l" then
            local e = avail:find("\n", self.pos, true)
            if e then
                local line = avail:sub(self.pos, e - 1)
                self.pos = e + 1
                return ((prefix .. line):gsub("\r", ""))
            end
        else
            local want = pat - #prefix
            if #avail - self.pos + 1 >= want then
                local s = avail:sub(self.pos, self.pos + want - 1)
                self.pos = self.pos + want
                return prefix .. s
            end
        end
        local r1, r2, r3 = short(self, prefix)
        if r2 then return r1, r2, r3 end
    end
end

return M
