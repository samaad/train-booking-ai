import { describe, expect, it } from 'vitest'
import { SseParser, toChatEvent } from './sse'

describe('SseParser', () => {
  it('parses frames split across chunks and CRLF line endings', () => {
    const p = new SseParser()
    expect(p.push('event:token\r\ndata:{"text":"Hel')).toEqual([])
    expect(p.push('lo"}\r\n\r\nevent:done\ndata:{"conversationId":"c1"}\n\n')).toEqual([
      { event: 'token', data: '{"text":"Hello"}' },
      { event: 'done', data: '{"conversationId":"c1"}' },
    ])
  })

  it('joins multi-line data, strips one leading space and skips comments', () => {
    const p = new SseParser()
    expect(p.push(': keep-alive\n\nevent: x\ndata: a\ndata:  b\n\n')).toEqual([{ event: 'x', data: 'a\n b' }])
  })
})

describe('toChatEvent', () => {
  it('maps known events and ignores unknown or malformed ones', () => {
    expect(toChatEvent({ event: 'token', data: '{"text":"hi"}' })).toEqual({ type: 'token', text: 'hi' })
    expect(toChatEvent({ event: 'future', data: '{}' })).toBeNull()
    expect(toChatEvent({ event: 'token', data: 'not json' })).toBeNull()
  })
})
