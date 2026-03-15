/**
 * Central shortcut config for the editor.
 *
 * Edit these bindings to customize the desktop review flow without touching the
 * keybinding command logic itself.
 */

export type ProofKeybindingConfig = {
  invokeAgent: string;
  addProofComment: string;
  undo: string;
  redo: string;
  nextComment: string;
  prevComment: string;
  nextSuggestion: string;
  prevSuggestion: string;
  acceptSuggestionAndNext: string;
  rejectSuggestionAndNext: string;
  toggleTrackChanges: string;
  resolveComment: string;
};

export const proofKeybindingConfig: Readonly<ProofKeybindingConfig> = {
  invokeAgent: 'Mod-Shift-p',
  addProofComment: 'Mod-Shift-k',
  undo: 'Mod-z',
  redo: 'Mod-Shift-z',
  nextComment: 'Mod-]',
  prevComment: 'Mod-[',
  nextSuggestion: 'Mod-Alt-]',
  prevSuggestion: 'Mod-Alt-[',
  acceptSuggestionAndNext: 'Mod-Alt-a',
  rejectSuggestionAndNext: 'Mod-Alt-r',
  toggleTrackChanges: 'Mod-Shift-e',
  resolveComment: 'Mod-Shift-r',
};
